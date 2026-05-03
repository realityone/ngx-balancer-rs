#!/usr/bin/perl

# Port of ingress-nginx's `test/e2e/loadbalance/ewma.go` "does not
# fail requests" check: with three healthy backends behind an ewma
# upstream, every sequential request should succeed. Distribution
# across peers is randomized (P2C samples 2 of 3 each pick), so the
# Go test asserts only that the total count matches — we mirror that
# and additionally check that more than one peer was actually used
# (otherwise the test would silently pass even if P2C degenerated).

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
        server 127.0.0.1:8081;
        server 127.0.0.1:8082;
        server 127.0.0.1:8083;
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
$t->run_daemon(\&http_daemon, port(8083));
$t->run();

$t->waitforsocket('127.0.0.1:' . port(8081));
$t->waitforsocket('127.0.0.1:' . port(8082));
$t->waitforsocket('127.0.0.1:' . port(8083));

###############################################################################

my %seen;
my $ok = 0;

for (1 .. 30) {
	my $resp = http_get('/');
	$ok++ if $resp =~ m{^HTTP/1\.1 200};
	$seen{$1}++ if $resp =~ /X-Port: (\d+)/;
}

is($ok, 30, 'ewma: 30/30 sequential requests succeed across 3 backends');
cmp_ok(scalar(keys %seen), '>=', 2,
	'ewma: traffic spread across more than one peer');

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
