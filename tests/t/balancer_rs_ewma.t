#!/usr/bin/perl

# End-to-end test for the `balancer_rs ewma` policy.
#
# Setup: two backends — 8081 always sleeps 100 ms before responding,
# 8082 responds immediately. After a short warmup (which seeds both
# peers' EWMA scores by P2C-sampling each at least once), the slow
# peer's score is ~100x the fast peer's. With only two peers, P2C
# samples both on every pick → it deterministically routes to the
# lower-scored peer.
#
# This is the EWMA discriminator: stock nginx least_conn with idle
# peers ties at conns=0 and falls to weighted round-robin (a 10/10
# split), whereas EWMA learns from completed RTT and prefers the
# faster peer almost exclusively.

use warnings;
use strict;

use Test::More;

BEGIN { use FindBin; chdir($FindBin::Bin); }

use lib '../nginx-tests/lib';
use Test::Nginx;

###############################################################################

select STDERR; $| = 1;
select STDOUT; $| = 1;

my $t = Test::Nginx->new()->has(qw/http proxy/)->plan(1);

$t->write_file_expand('nginx.conf', <<'EOF');

%%TEST_GLOBALS%%

daemon off;

events {
}

http {
    %%TEST_GLOBALS_HTTP%%

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

my ($slow_port, $fast_port) = (port(8081), port(8082));

# Warmup: seed both peers' EWMA. Sequentially issuing 4 requests
# guarantees P2C touches each peer at least once regardless of how
# the first random sample falls.
for (1 .. 4) {
	http_get('/');
}

my $fast_count = 0;
for (1 .. 20) {
	if (http_get('/') =~ /X-Port: (\d+)/) {
		$fast_count++ if $1 == $fast_port;
	}
}

cmp_ok($fast_count, '>=', 18,
	"ewma: fast peer ($fast_port) served $fast_count/20 main requests");

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

		# 8081 is the slow peer — every response stalls 100ms so its
		# EWMA score grows ~100x the fast peer's after one sample.
		if ($port == port(8081)) {
			select undef, undef, undef, 0.1;
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
