#!/usr/bin/perl

# Zone-mode smoke for `balancer_rs ewma`. Adds the `zone NAME SIZE;`
# directive on the upstream block, which routes round_robin's peer
# state into a shared-memory slab pool — and triggers our own
# `ngx_shm_zone_t` registration + `ewma_zone_init` callback path
# (allocations land in shared memory before fork).
#
# Without simulating DNS-driven peer churn we can't exercise the
# resync code path from Test::Nginx, so this test asserts the
# smaller (but still valuable) thing: nginx accepts the config,
# starts cleanly, and a sequence of requests goes through the
# ewma upstream successfully — ruling out any obvious crash or
# init-order regression in the shared-zone setup.

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

    upstream u {
        zone u 1m;
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

my $ok = 0;
my %seen;
for (1 .. 16) {
    my $resp = http_get('/');
    $ok++ if $resp =~ m{^HTTP/1\.1 200};
    $seen{$1}++ if $resp =~ /X-Port: (\d+)/;
}

is($ok, 16, 'ewma zone-mode: 16/16 sequential requests succeed');
cmp_ok(scalar(keys %seen), '>=', 1,
    'ewma zone-mode: at least one peer served traffic');

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
