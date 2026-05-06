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
pub(super) struct EwmaStatus {
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

type PoolEwmaSlotIndex = RbTreeMap<SockaddrKey, *mut EwmaStatus, Pool>;
type SlabEwmaSlotIndex = RbTreeMap<SockaddrKey, *mut EwmaStatus, SlabPool>;

/// Raw EWMA slot table owned by an nginx pool or slab pool.
///
/// The pointer remains raw because nginx owns the allocation lifetime,
/// but grouping it with `len` keeps callers from passing mismatched
/// pointer/length pairs. Iteration-heavy code should use `as_slice`
/// or `as_mut_slice` so slot table reads/writes look like normal
/// slice access at the call site. `find_slot_by_sockaddr` always uses
/// one of the rb-tree indexes; static upstreams use `pool_index`, and
/// zone upstreams use `slab_index`.
#[derive(Clone, Copy)]
#[repr(C)]
pub(super) struct EwmaSlotTable {
    slots: NonNull<[EwmaStatus]>,
    pool_index: *mut PoolEwmaSlotIndex,
    slab_index: *mut SlabEwmaSlotIndex,
}

impl EwmaSlotTable {
    fn from_raw(ptr: *mut EwmaStatus, len: usize) -> Option<Self> {
        let data = NonNull::new(ptr)?;
        Some(Self {
            slots: NonNull::slice_from_raw_parts(data, len),
            pool_index: ptr::null_mut(),
            slab_index: ptr::null_mut(),
        })
    }

    pub(super) fn empty() -> Self {
        Self {
            slots: NonNull::slice_from_raw_parts(NonNull::dangling(), 0),
            pool_index: ptr::null_mut(),
            slab_index: ptr::null_mut(),
        }
    }

    fn with_pool_index(mut self, index: *mut PoolEwmaSlotIndex) -> Self {
        self.pool_index = index;
        self
    }

    fn with_slab_index(mut self, index: *mut SlabEwmaSlotIndex) -> Self {
        self.slab_index = index;
        self
    }

    pub(super) fn len(&self) -> ngx_uint_t {
        self.as_slice().len()
    }

    fn as_slice(&self) -> &[EwmaStatus] {
        unsafe { self.slots.as_ref() }
    }

    pub(super) fn as_mut_slice(&mut self) -> &mut [EwmaStatus] {
        unsafe { self.slots.as_mut() }
    }

    pub(super) fn get_mut(&mut self, i: usize) -> Option<&mut EwmaStatus> {
        self.as_mut_slice().get_mut(i)
    }

    pub(super) fn index_allocator(&self) -> Option<SlabPool> {
        unsafe { self.slab_index.as_ref() }.map(|index| index.allocator().clone())
    }

    pub(super) fn index_slot_at(
        &self,
        i: usize,
        sockaddr: *mut sockaddr,
        socklen: socklen_t,
    ) -> Result<(), ()> {
        let Some(slot) = self.as_slice().get(i) else {
            return Err(());
        };
        let raw_slot = ptr::from_ref(slot).cast_mut();
        index_slot(
            self.pool_index,
            self.slab_index,
            sockaddr,
            socklen,
            raw_slot,
        )
    }

    fn get_by_sockaddr(&self, sockaddr: *mut sockaddr, socklen: socklen_t) -> *mut EwmaStatus {
        let Some(key) = SockaddrKey::from_raw(sockaddr, socklen) else {
            return ptr::null_mut();
        };

        if let Some(index) = unsafe { self.pool_index.as_ref() } {
            return index.get(&key).copied().unwrap_or(ptr::null_mut());
        }

        if let Some(index) = unsafe { self.slab_index.as_ref() } {
            return index.get(&key).copied().unwrap_or(ptr::null_mut());
        }

        ptr::null_mut()
    }
}

/// Allocate a zero-initialized `EwmaStatus` array from `pool`. Returns
/// `None` only when the allocation fails for a non-zero count; a
/// zero-length list yields a null pointer (callers must gate reads
/// on `len > 0`).
pub(super) fn alloc_slots(pool: &Pool, len: ngx_uint_t) -> Option<EwmaSlotTable> {
    let index = alloc_pool_slot_index(pool)?;
    if len == 0 {
        return Some(EwmaSlotTable::empty().with_pool_index(index));
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaStatus>())?;
    let p = pool.calloc(bytes).cast::<EwmaStatus>();
    if p.is_null() {
        None
    } else {
        Some(EwmaSlotTable::from_raw(p, len)?.with_pool_index(index))
    }
}

/// Slab-allocate a zero-initialized `EwmaStatus` array. Caller is
/// responsible for ensuring the slab pool is either uncontended
/// (zone init) or that `shpool->mutex` is held (runtime resync).
unsafe fn alloc_slots_shpool_locked(
    shpool: *mut ngx_slab_pool_t,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    if len == 0 {
        return Some(EwmaSlotTable::empty());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaStatus>())?;
    let p = unsafe { ngx_slab_calloc_locked(shpool, bytes) }.cast::<EwmaStatus>();
    if p.is_null() {
        None
    } else {
        Some(EwmaSlotTable::from_raw(p, len)?)
    }
}

/// Slab-allocate a zero-initialized `EwmaStatus` array using the normal
/// slab allocator entry point, which takes `shpool->mutex` internally.
unsafe fn alloc_slots_shpool(
    shpool: *mut ngx_slab_pool_t,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    if len == 0 {
        return Some(EwmaSlotTable::empty());
    }
    let bytes = len.checked_mul(core::mem::size_of::<EwmaStatus>())?;
    let p = unsafe { ngx_slab_calloc(shpool, bytes) }.cast::<EwmaStatus>();
    if p.is_null() {
        None
    } else {
        Some(EwmaSlotTable::from_raw(p, len)?)
    }
}

pub(super) unsafe fn alloc_indexed_slots_shpool_locked(
    shpool: *mut ngx_slab_pool_t,
    alloc: &SlabPool,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    let index = alloc_slab_slot_index(alloc)?;
    let Some(slots) = (unsafe { alloc_slots_shpool_locked(shpool, len) }) else {
        free_slab_slot_index(index);
        return None;
    };
    Some(slots.with_slab_index(index))
}

pub(super) unsafe fn alloc_indexed_slots_shpool(
    shpool: *mut ngx_slab_pool_t,
    alloc: &SlabPool,
    len: ngx_uint_t,
) -> Option<EwmaSlotTable> {
    let index = alloc_slab_slot_index(alloc)?;
    let Some(slots) = (unsafe { alloc_slots_shpool(shpool, len) }) else {
        free_slab_slot_index(index);
        return None;
    };
    Some(slots.with_slab_index(index))
}

fn alloc_pool_slot_index(alloc: &Pool) -> Option<*mut PoolEwmaSlotIndex> {
    let map = PoolEwmaSlotIndex::try_new_in(alloc.clone()).ok()?;
    let layout = Layout::new::<PoolEwmaSlotIndex>();
    let ptr: NonNull<PoolEwmaSlotIndex> = alloc.allocate_zeroed(layout).ok()?.cast();
    unsafe { ptr.as_ptr().write(map) };
    Some(ptr.as_ptr())
}

fn alloc_slab_slot_index(alloc: &SlabPool) -> Option<*mut SlabEwmaSlotIndex> {
    let map = SlabEwmaSlotIndex::try_new_in(alloc.clone()).ok()?;
    let layout = Layout::new::<SlabEwmaSlotIndex>();
    let ptr: NonNull<SlabEwmaSlotIndex> = alloc.allocate_zeroed(layout).ok()?.cast();
    unsafe { ptr.as_ptr().write(map) };
    Some(ptr.as_ptr())
}

fn free_slab_slot_index(index: *mut SlabEwmaSlotIndex) {
    let Some(index_ref) = (unsafe { index.as_ref() }) else {
        return;
    };
    let alloc = index_ref.allocator().clone();
    let layout = Layout::new::<SlabEwmaSlotIndex>();
    unsafe {
        ptr::drop_in_place(index);
        alloc.deallocate(NonNull::new_unchecked(index.cast::<u8>()), layout);
    }
}

pub(super) fn free_slab_slot_table(shpool: *mut ngx_slab_pool_t, slots: EwmaSlotTable) {
    free_slab_slot_index(slots.slab_index);
    if slots.len() != 0 {
        let ptr = slots.as_slice().as_ptr().cast_mut();
        unsafe { ngx_slab_free(shpool, ptr.cast()) };
    }
}

fn index_slot(
    pool_index: *mut PoolEwmaSlotIndex,
    slab_index: *mut SlabEwmaSlotIndex,
    sockaddr: *mut sockaddr,
    socklen: socklen_t,
    slot: *mut EwmaStatus,
) -> Result<(), ()> {
    let Some(key) = SockaddrKey::from_raw(sockaddr, socklen) else {
        return Err(());
    };

    if let Some(index) = unsafe { pool_index.as_mut() } {
        return index.try_insert(key, slot).map(|_| ()).map_err(|_| ());
    }

    if let Some(index) = unsafe { slab_index.as_mut() } {
        return index.try_insert(key, slot).map(|_| ()).map_err(|_| ());
    }

    Err(())
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
    let pool_index = slots.pool_index;
    let slab_index = slots.slab_index;
    for slot in slots.as_mut_slice() {
        let Some(peer) = (unsafe { peer_ptr.as_ref() }) else {
            break;
        };
        slot.sockaddr = peer.sockaddr;
        slot.socklen = peer.socklen;
        let raw_slot = ptr::from_mut(slot);
        index_slot(
            pool_index,
            slab_index,
            peer.sockaddr,
            peer.socklen,
            raw_slot,
        )?;
        peer_ptr = peer.next;
    }
    Ok(())
}

/// Find the slot owning `(sockaddr, socklen)` through the rb-tree
/// index built when slot identities are stamped.
pub(super) fn find_slot_by_sockaddr(
    slots: EwmaSlotTable,
    sockaddr: *mut sockaddr,
    socklen: socklen_t,
) -> *mut EwmaStatus {
    slots.get_by_sockaddr(sockaddr, socklen)
}

/// Read `slot` and return its EWMA decayed forward to `now`. A null
/// pointer or a freshly-zeroed slot scores 0.
#[allow(clippy::cast_precision_loss)]
pub(super) fn decay_score(slot: *mut EwmaStatus, now_msec: ngx_msec_t) -> f64 {
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
pub(super) fn ewma_update(slot: *mut EwmaStatus, rtt_msec: ngx_msec_t, now_msec: ngx_msec_t) {
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
