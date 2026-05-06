use core::{alloc::Layout, ptr, ptr::NonNull};

use ngx::{
    allocator::Allocator,
    collections::RbTreeMap,
    core::{Pool, SlabPool},
    ffi::{
        ngx_http_upstream_rr_peers_t, ngx_msec_t, ngx_slab_calloc, ngx_slab_calloc_locked,
        ngx_slab_free, ngx_slab_pool_t, ngx_uint_t, sockaddr, sockaddr_storage, socklen_t,
    },
};

/// EWMA decay time constant in milliseconds. A sample 10s after the
/// previous one contributes ~63% to the new score; one a millisecond
/// later moves the score by ~0.01%. Matches ingress-nginx's
/// `DECAY_TIME = 10` (seconds).
const DECAY_MSEC: f64 = 10_000.0;

#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct EwmaSlot {
    /// Identity of the peer this slot belongs to. Sockaddr is the
    /// stable key — peer linked-list positions shift when zone-driven
    /// peer churn removes entries (see
    /// `ngx_http_upstream_zone_module.c:ngx_http_upstream_zone_remove_peer_locked`).
    /// Non-zone configs are static, so the pointer aliases the peer's
    /// own `sockaddr` for the cycle's lifetime.
    pub(super) sockaddr: *mut sockaddr,
    pub(super) socklen: socklen_t,
    pub(super) ewma: f64,
    pub(super) last_touched_msec: ngx_msec_t,
}

const SOCKADDR_KEY_BYTES: usize = core::mem::size_of::<sockaddr_storage>();

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct SockaddrKey {
    len: socklen_t,
    bytes: [u8; SOCKADDR_KEY_BYTES],
}

impl SockaddrKey {
    fn from_raw(sockaddr: *mut sockaddr, socklen: socklen_t) -> Option<Self> {
        if sockaddr.is_null() || socklen == 0 {
            return None;
        }

        let len = usize::try_from(socklen).ok()?;
        if len > SOCKADDR_KEY_BYTES {
            return None;
        }

        let mut bytes = [0u8; SOCKADDR_KEY_BYTES];
        unsafe { ptr::copy_nonoverlapping(sockaddr.cast::<u8>(), bytes.as_mut_ptr(), len) };
        Some(Self {
            len: socklen,
            bytes,
        })
    }
}

type EwmaSlotIndex = RbTreeMap<SockaddrKey, *mut EwmaSlot, SlabPool>;

/// Raw EWMA slot table owned by an nginx pool or slab pool.
///
/// The pointer remains raw because nginx owns the allocation lifetime,
/// but grouping it with `len` keeps callers from passing mismatched
/// pointer/length pairs. Iteration-heavy code should use `as_slice`
/// or `as_mut_slice` so slot table reads/writes look like normal
/// slice access at the call site. In zone mode `index` points at a
/// slab-backed `RbTreeMap` from copied sockaddr bytes to slot pointers;
/// static upstreams leave it null and use the slice as the lookup
/// source.
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub(super) struct EwmaSlotTable {
    ptr: *mut EwmaSlot,
    len: ngx_uint_t,
    index: *mut EwmaSlotIndex,
}

impl EwmaSlotTable {
    pub(super) fn new(ptr: *mut EwmaSlot, len: ngx_uint_t) -> Self {
        Self {
            ptr,
            len,
            index: ptr::null_mut(),
        }
    }

    pub(super) fn empty() -> Self {
        Self::new(ptr::null_mut(), 0)
    }

    fn with_index(mut self, index: *mut EwmaSlotIndex) -> Self {
        self.index = index;
        self
    }

    pub(super) fn len(&self) -> ngx_uint_t {
        self.len
    }

    fn as_slice(&self) -> &[EwmaSlot] {
        if self.ptr.is_null() || self.len == 0 {
            return &[];
        }
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [EwmaSlot] {
        if self.ptr.is_null() || self.len == 0 {
            return &mut [];
        }
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub(super) fn get_mut(&mut self, i: usize) -> Option<&mut EwmaSlot> {
        self.as_mut_slice().get_mut(i)
    }

    pub(super) fn index_allocator(&self) -> Option<SlabPool> {
        unsafe { self.index.as_ref() }.map(|index| index.allocator().clone())
    }

    pub(super) fn index_slot_at(
        &self,
        i: usize,
        sockaddr: *mut sockaddr,
        socklen: socklen_t,
    ) -> Result<(), ()> {
        let raw_slot = unsafe { self.ptr.add(i) };
        index_slot(self.index, sockaddr, socklen, raw_slot)
    }
}

/// Allocate a zero-initialized `EwmaSlot` array from `pool`. Returns
/// `None` only when the allocation fails for a non-zero count; a
/// zero-length list yields a null pointer (callers must gate reads
/// on `len > 0`).
pub(super) fn alloc_slots(pool: &Pool, len: ngx_uint_t) -> Option<EwmaSlotTable> {
    if len == 0 {
        return Some(EwmaSlotTable::empty());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaSlot>())?;
    let p = pool.calloc(bytes).cast::<EwmaSlot>();
    if p.is_null() {
        None
    } else {
        Some(EwmaSlotTable::new(p, len))
    }
}

/// Slab-allocate a zero-initialized `EwmaSlot` array. Caller is
/// responsible for ensuring the slab pool is either uncontended
/// (zone init) or that `shpool->mutex` is held (runtime resync).
unsafe fn alloc_slots_shpool_locked(
    shpool: *mut ngx_slab_pool_t,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    if len == 0 {
        return Some(EwmaSlotTable::empty());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaSlot>())?;
    let p = unsafe { ngx_slab_calloc_locked(shpool, bytes) }.cast::<EwmaSlot>();
    if p.is_null() {
        None
    } else {
        Some(EwmaSlotTable::new(p, len))
    }
}

/// Slab-allocate a zero-initialized `EwmaSlot` array using the normal
/// slab allocator entry point, which takes `shpool->mutex` internally.
unsafe fn alloc_slots_shpool(
    shpool: *mut ngx_slab_pool_t,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    if len == 0 {
        return Some(EwmaSlotTable::empty());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaSlot>())?;
    let p = unsafe { ngx_slab_calloc(shpool, bytes) }.cast::<EwmaSlot>();
    if p.is_null() {
        None
    } else {
        Some(EwmaSlotTable::new(p, len))
    }
}

pub(super) unsafe fn alloc_indexed_slots_shpool_locked(
    shpool: *mut ngx_slab_pool_t,
    alloc: &SlabPool,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    let index = alloc_slot_index(alloc)?;
    let Some(slots) = (unsafe { alloc_slots_shpool_locked(shpool, len) }) else {
        free_slot_index(index);
        return None;
    };
    Some(slots.with_index(index))
}

pub(super) unsafe fn alloc_indexed_slots_shpool(
    shpool: *mut ngx_slab_pool_t,
    alloc: &SlabPool,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    let index = alloc_slot_index(alloc)?;
    let Some(slots) = (unsafe { alloc_slots_shpool(shpool, len) }) else {
        free_slot_index(index);
        return None;
    };
    Some(slots.with_index(index))
}

fn alloc_slot_index(alloc: &SlabPool) -> Option<*mut EwmaSlotIndex> {
    let map = EwmaSlotIndex::try_new_in(alloc.clone()).ok()?;
    let layout = Layout::new::<EwmaSlotIndex>();
    let ptr: NonNull<EwmaSlotIndex> = alloc.allocate_zeroed(layout).ok()?.cast();
    unsafe { ptr.as_ptr().write(map) };
    Some(ptr.as_ptr())
}

fn free_slot_index(index: *mut EwmaSlotIndex) {
    let Some(index_ref) = (unsafe { index.as_ref() }) else {
        return;
    };
    let alloc = index_ref.allocator().clone();
    let layout = Layout::new::<EwmaSlotIndex>();
    unsafe {
        ptr::drop_in_place(index);
        alloc.deallocate(NonNull::new_unchecked(index.cast::<u8>()), layout);
    }
}

pub(super) fn free_slab_slot_table(shpool: *mut ngx_slab_pool_t, slots: EwmaSlotTable) {
    free_slot_index(slots.index);
    if !slots.ptr.is_null() {
        unsafe { ngx_slab_free(shpool, slots.ptr.cast()) };
    }
}

fn index_slot(
    index: *mut EwmaSlotIndex,
    sockaddr: *mut sockaddr,
    socklen: socklen_t,
    slot: *mut EwmaSlot,
) -> Result<(), ()> {
    let Some(index) = (unsafe { index.as_mut() }) else {
        return Ok(());
    };
    let Some(key) = SockaddrKey::from_raw(sockaddr, socklen) else {
        return Err(());
    };
    index.try_insert(key, slot).map(|_| ()).map_err(|_| ())
}

/// Walk `peers` and copy each peer's `(sockaddr, socklen)` into the
/// matching slot. Slots and peers are 1:1 at init time; once peer
/// churn shifts linked-list positions, slots are looked up by sockaddr
/// through the optional slab-backed index, so this initial alignment
/// doesn't have to hold forever.
pub(super) fn stamp_slot_identities(
    peers: &ngx_http_upstream_rr_peers_t,
    slots: &mut EwmaSlotTable,
) -> Result<(), ()> {
    let mut peer_ptr = peers.peer;
    let slots_base = slots.ptr;
    let index = slots.index;
    for (i, slot) in slots.as_mut_slice().iter_mut().enumerate() {
        let Some(peer) = (unsafe { peer_ptr.as_ref() }) else {
            break;
        };
        slot.sockaddr = peer.sockaddr;
        slot.socklen = peer.socklen;
        let raw_slot = unsafe { slots_base.add(i) };
        index_slot(index, peer.sockaddr, peer.socklen, raw_slot)?;
        peer_ptr = peer.next;
    }
    Ok(())
}

/// Find the slot owning `(sockaddr, socklen)`. Zone-mode tables use
/// the slab-backed `RbTreeMap` index; static tables keep the older
/// slice scan because they do not have a slab allocator.
pub(super) fn find_slot_by_sockaddr(
    slots: EwmaSlotTable,
    sockaddr: *mut sockaddr,
    socklen: socklen_t,
) -> *mut EwmaSlot {
    let Some(key) = SockaddrKey::from_raw(sockaddr, socklen) else {
        return ptr::null_mut();
    };

    if let Some(index) = unsafe { slots.index.as_ref() } {
        return index.get(&key).copied().unwrap_or(ptr::null_mut());
    }

    let Ok(n) = usize::try_from(socklen) else {
        return ptr::null_mut();
    };
    let needle = &key.bytes[..n];
    for (i, slot) in slots.as_slice().iter().enumerate() {
        if slot.sockaddr.is_null() || slot.socklen != socklen {
            continue;
        }
        let haystack = unsafe { core::slice::from_raw_parts(slot.sockaddr.cast::<u8>(), n) };
        if haystack == needle {
            return unsafe { slots.ptr.add(i) };
        }
    }
    ptr::null_mut()
}

/// Read `slot` and return its EWMA decayed forward to `now`. A null
/// pointer or a freshly-zeroed slot scores 0.
#[allow(clippy::cast_precision_loss)]
pub(super) fn decay_score(slot: *mut EwmaSlot, now_msec: ngx_msec_t) -> f64 {
    if slot.is_null() {
        return 0.0;
    }
    let s = unsafe { *slot };
    let td = now_msec.saturating_sub(s.last_touched_msec);
    let weight = (-(td as f64) / DECAY_MSEC).exp();
    s.ewma * weight
}

/// Average decayed EWMA across slots that have at least one
/// recorded sample (`last_touched_msec != 0`). Used as the
/// slow-start seed for newly-appearing peers — keeps P2C from
/// always-picking the new peer because its zero score makes it
/// look "fastest." Returns 0.0 when no slot has been touched yet.
#[allow(clippy::cast_precision_loss)]
pub(super) fn slow_start_mean(slots: EwmaSlotTable, now_msec: ngx_msec_t) -> f64 {
    let mut sum = 0.0f64;
    let mut count = 0u64;
    for s in slots.as_slice() {
        if s.last_touched_msec == 0 {
            continue;
        }
        let td = now_msec.saturating_sub(s.last_touched_msec);
        let weight = (-(td as f64) / DECAY_MSEC).exp();
        sum += s.ewma * weight;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        sum / count as f64
    }
}

/// Apply the standard EWMA recurrence `ewma = ewma*w + rtt*(1-w)` in
/// place, with `w = exp(-td/DECAY)`. Bumps `last_touched_msec` to now.
#[allow(clippy::cast_precision_loss)]
pub(super) fn ewma_update(slot: *mut EwmaSlot, rtt_msec: ngx_msec_t, now_msec: ngx_msec_t) {
    if slot.is_null() {
        return;
    }
    let prev = unsafe { *slot };
    let td = now_msec.saturating_sub(prev.last_touched_msec);
    let weight = (-(td as f64) / DECAY_MSEC).exp();
    let new_ewma = prev.ewma * weight + (rtt_msec as f64) * (1.0 - weight);
    unsafe {
        (*slot).ewma = new_ewma;
        (*slot).last_touched_msec = now_msec;
    }
}
