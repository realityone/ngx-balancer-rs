#!/usr/bin/perl

# Verify that `$balancer_ewma_score` lands in nginx access logs after a
# request goes through an `ewma`-balanced upstream. We don't assert any
# particular numeric value (the score depends on observed RTTs and a
# 10s decay window), only that:
#   - some access-log lines carry a non-empty score,
#   - all scores parse as non-negative floats.

use warnings;
use strict;

use Test::More;

BEGIN { use FindBin; chdir($FindBin::Bin); }

use lib '../nginx-tests/lib';
use Test::Nginx;

###############################################################################

select STDERR; $| = 1;
select STDOUT; $| = 1;

my $t = Test::Nginx->new()->has(qw/http proxy/)->plan(2);

$t->write_file_expand('nginx.conf', <<'EOF');

%%TEST_GLOBALS%%

daemon off;

events {
}

http {
    %%TEST_GLOBALS_HTTP%%

    log_format ewma_log '$status $upstream_addr score=$balancer_ewma_score';
    access_log %%TESTDIR%%/access.log ewma_log;

    upstream u {
        balancer_rs ewma;
        server 127.0.0.1:8081;
        server 127.0.0.1:8082;
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

$t->run_daemon(\&http_daemon, port(8081));
$t->run_daemon(\&http_daemon, port(8082));
$t->run();

$t->waitforsocket('127.0.0.1:' . port(8081));
$t->waitforsocket('127.0.0.1:' . port(8082));

###############################################################################

# Warmup so the score is meaningful (each peer gets at least one
# completed request → its slot has a non-zero last_touched_msec).
http_get('/') for 1 .. 4;

# Drive a few more requests; these are the ones we'll grep the log for.
http_get('/') for 1 .. 8;

# Give Test::Nginx a moment to flush access logs to disk.
select undef, undef, undef, 0.2;

my $log = $t->read_file('access.log');
my @scores = ($log =~ /score=([^\s]+)/g);

cmp_ok(scalar(@scores), '>=', 8,
    'access log captured at least 8 score lines');

my $bad = 0;
for my $s (@scores) {
    if ($s !~ /^\d+(?:\.\d+)?$/ || $s + 0 < 0) {
        diag("unexpected balancer_ewma_score value: $s");
        $bad++;
    }
}
is($bad, 0, 'all scores parse as non-negative numbers');

###############################################################################

sub http_daemon {
    my ($port) = @_;

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

        my $headers = '';
        while (<$client>) {
            $headers .= $_;
            last if (/^\x0d?\x0a?$/);
        }

        Test::Nginx::log_core('||', "$port: response, 200");
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
