#!/usr/bin/perl
# Chaos HTTP backend for the balancer_rs fuzz harness.
#
# Listens on --port; on each request, rolls a die against the
# weights below and replies (or misbehaves) accordingly. Mischief
# is reproducible via --seed.

use warnings;
use strict;

use Getopt::Long;
use IO::Socket::INET;
use Time::HiRes qw/sleep/;

my $port;
my $seed = 0;
GetOptions(
    'port=i' => \$port,
    'seed=i' => \$seed,
) or die "usage: $0 --port=PORT [--seed=N]\n";
defined $port or die "--port is required\n";

# XOR the global seed with the port so each backend gets a distinct
# but reproducible stream of decisions.
srand($seed ^ $port);

my $server = IO::Socket::INET->new(
    Proto     => 'tcp',
    LocalHost => '127.0.0.1',
    LocalPort => $port,
    Listen    => 64,
    Reuse     => 1,
) or die "chaos_backend[$port]: bind failed: $!\n";

local $SIG{PIPE} = 'IGNORE';
local $SIG{TERM} = sub { exit 0 };
local $SIG{INT}  = sub { exit 0 };
$| = 1;

print STDERR "chaos_backend: listening on 127.0.0.1:$port (seed=$seed)\n";

while (my $client = $server->accept()) {
    $client->autoflush(1);

    my $headers = '';
    while (my $line = <$client>) {
        $headers .= $line;
        last if ($line =~ /^\x0d?\x0a?$/);
    }

    my $r = rand();
    if ($r < 0.60) {
        send_ok($client);
    }
    elsif ($r < 0.75) {
        sleep(0.05 + rand(0.45));
        send_ok($client);
    }
    elsif ($r < 0.85) {
        send_502($client);
    }
    elsif ($r < 0.93) {
        # Partial header send, then close.
        print $client "HTTP/1.1 200 OK\r\nServer: chaos\r\nX-Po";
    }
    elsif ($r < 0.98) {
        # Full headers, partial body, then close.
        print $client "HTTP/1.1 200 OK\r\n";
        print $client "Connection: close\r\n";
        print $client "Content-Length: 4096\r\n";
        print $client "X-Port: $port\r\n";
        print $client "X-Backend: trunc\r\n";
        print $client "\r\n";
        print $client "x" x 12;
    }
    else {
        # Hang past nginx's proxy_read_timeout (5s in fuzz config).
        sleep(30);
    }

    close $client;
}

sub send_ok {
    my ($client) = @_;
    my $body = "OK";
    print $client "HTTP/1.1 200 OK\r\n",
        "Connection: close\r\n",
        "Content-Length: ", length($body), "\r\n",
        "X-Port: $port\r\n",
        "X-Backend: ok\r\n\r\n",
        $body;
}

sub send_502 {
    my ($client) = @_;
    my $body = "bad-gtwy";
    print $client "HTTP/1.1 502 Bad Gateway\r\n",
        "Connection: close\r\n",
        "Content-Length: ", length($body), "\r\n",
        "X-Port: $port\r\n",
        "X-Backend: 502\r\n\r\n",
        $body;
}
