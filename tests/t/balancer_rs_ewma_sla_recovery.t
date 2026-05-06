#!/usr/bin/perl

# EWMA recovery test with five peers. All peers start healthy, then
# one backend begins sleeping long enough to violate the request SLA.
# Once EWMA observes that slow sample, P2C should route around the
# degraded peer and the aggregate latency should recover.

use warnings;
use strict;

use Time::HiRes qw(time);
use Test::More;

BEGIN { use FindBin; chdir($FindBin::Bin); }

use lib '../nginx-tests/lib';
use Test::Nginx;

###############################################################################

select STDERR; $| = 1;
select STDOUT; $| = 1;

my $t = Test::Nginx->new()->has(qw/http proxy/)->plan(7);

$t->write_file_expand('nginx.conf', <<'EOF');

%%TEST_GLOBALS%%

daemon off;

events {
}

http {
    %%TEST_GLOBALS_HTTP%%

    log_format ewma_log '$status $upstream_addr score=$balancer_ewma_score';
    access_log %%TESTDIR%%/ewma_sla_recovery_access.log ewma_log;

    upstream u {
        balancer_rs ewma;
        server 127.0.0.1:8081;
        server 127.0.0.1:8082;
        server 127.0.0.1:8083;
        server 127.0.0.1:8084;
        server 127.0.0.1:8085;
    }

    server {
        listen       127.0.0.1:8080;
        server_name  localhost;

        location / {
            proxy_pass http://u;
        }
    }
}

EOF

diag('ewma recovery access log: ' . $t->testdir() . '/ewma_sla_recovery_access.log');

my $bad_flag = $t->testdir() . '/bad-peer-on';
my $bad_port = port(8085);

for my $base (8081 .. 8085) {
    $t->run_daemon(\&http_daemon, port($base), $bad_port, $bad_flag);
}

$t->run();

for my $base (8081 .. 8085) {
    $t->waitforsocket('127.0.0.1:' . port($base));
}

###############################################################################

my ($ok, $seen, $latencies) = request_batch(50);

is($ok, 50, 'ewma recovery: all warmup requests succeed across 5 peers');
cmp_ok(scalar(keys %$seen), '>=', 4,
    'ewma recovery: healthy warmup reaches most peers');

$t->write_file('bad-peer-on', '1');

my ($fault_seen, $fault_latency) = (0, 0);
for (1 .. 80) {
    my ($resp, $elapsed) = request_once();
    if ($resp =~ /X-Port: (\d+)/ && $1 == $bad_port) {
        $fault_seen = 1;
        $fault_latency = $elapsed;
        last;
    }
}

ok($fault_seen, 'ewma recovery: degraded peer is sampled after it goes bad');
cmp_ok($fault_latency, '>=', 0.20,
    'ewma recovery: degraded peer sample violates the SLA');

($ok, $seen, $latencies) = request_batch(80);

my $bad_after_recovery = $seen->{$bad_port} || 0;
my $p95 = percentile($latencies, 0.95);

cmp_ok($bad_after_recovery, '<=', 3,
    "ewma recovery: degraded peer served only $bad_after_recovery/80 recovery requests");
cmp_ok($p95, '<', 0.15,
    sprintf('ewma recovery: p95 latency recovered below SLA (%.3fs)', $p95));

select undef, undef, undef, 0.2;
my $log = $t->read_file('ewma_sla_recovery_access.log');
my @numeric_scores = ($log =~ /score=(\d+(?:\.\d+)?)/g);

cmp_ok(scalar(@numeric_scores), '>=', 80,
    'ewma recovery: access log captured numeric ewma scores');

###############################################################################

sub request_once {
    my $start = time();
    my $resp = http_get('/');
    return ($resp, time() - $start);
}

sub request_batch {
    my ($count) = @_;

    my $ok = 0;
    my %seen;
    my @latencies;

    for (1 .. $count) {
        my ($resp, $elapsed) = request_once();
        push @latencies, $elapsed;
        $ok++ if $resp =~ m{^HTTP/1\.1 200};
        $seen{$1}++ if $resp =~ /X-Port: (\d+)/;
    }

    return ($ok, \%seen, \@latencies);
}

sub percentile {
    my ($values, $q) = @_;

    my @sorted = sort { $a <=> $b } @$values;
    return 0 if !@sorted;

    my $i = int($q * ($#sorted));
    return $sorted[$i];
}

sub http_daemon {
    my ($port, $bad_port, $bad_flag) = @_;

    my $server = IO::Socket::INET->new(
        Proto     => 'tcp',
        LocalHost => '127.0.0.1',
        LocalPort => $port,
        Listen    => 5,
        Reuse     => 1,
    ) or die "Can't create listening socket: $!\n";

    local $SIG{PIPE} = 'IGNORE';

    while (my $client = $server->accept()) {
        $client->autoflush(1);

        while (<$client>) {
            last if (/^\x0d?\x0a?$/);
        }

        my $delay = (-e $bad_flag && $port == $bad_port) ? 0.25 : 0;
        select undef, undef, undef, $delay if $delay;

        Test::Nginx::log_core('||', "$port: response, 200, delay=$delay");
        print $client <<EOF;
HTTP/1.1 200 OK
Connection: close
X-Port: $port

OK
EOF

        close $client;
    }
}

###############################################################################
