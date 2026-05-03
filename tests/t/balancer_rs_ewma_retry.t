#!/usr/bin/perl

# Port of ingress-nginx's "doesn't pick the tried endpoint while
# retry" Lua unit test (rootfs/.../lua/test/balancer/ewma_test.lua):
# when a peer fails, the per-request `tried[]` bitmap (which our
# `peer_available` helper honors, inherited from round_robin) keeps
# the retry from picking the dead peer again. round_robin's
# `max_fails=1 fail_timeout=10s` defaults additionally quarantine
# the dead peer for subsequent requests, so 19 of the 20 requests
# go straight to the live one with no retry needed.

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
        balancer_rs ewma;
        # 8081 has no listener; the kernel rejects connect() with
        # ECONNREFUSED, which nginx surfaces as NGX_PEER_FAILED.
        server 127.0.0.1:8081;
        server 127.0.0.1:8082;
    }

    server {
        listen       127.0.0.1:8080;
        server_name  localhost;

        proxy_next_upstream error timeout;

        location / {
            proxy_pass http://u;
        }
    }
}

EOF

$t->run_daemon(\&http_daemon, port(8082));
$t->run();

$t->waitforsocket('127.0.0.1:' . port(8082));

###############################################################################

my $live_port = port(8082);
my $ok = 0;
my $live = 0;

for (1 .. 20) {
	my $resp = http_get('/');
	$ok++ if $resp =~ m{^HTTP/1\.1 200};
	$live++ if $resp =~ /X-Port: (\d+)/ && $1 == $live_port;
}

is($ok, 20, 'ewma: 20/20 requests succeed despite a dead peer');
is($live, 20, "ewma: every response served by the live peer ($live_port)");

###############################################################################

sub http_daemon {
	my ($port) = @_;

	my $server = IO::Socket::INET->new(
		Proto => 'tcp',
		LocalHost => '127.0.0.1',
		LocalPort => $port,
		Listen => 5,
		Reuse => 1
	)
		or die "Can't create listening socket: $!\n";

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
