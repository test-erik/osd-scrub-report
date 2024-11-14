#!/usr/bin/perl
use strict;
use warnings;
use feature qw(say switch);
use JSON;
use POSIX qw(ceil);
use Time::Local;
use experimental 'smartmatch';

# Dump the ceph pg stats to a JSON file
my $cmd = "ceph -f json pg dump pgs 2>&1 > /tmp/pgs_dump.json";
my $status = system($cmd);

if ($status != 0) {
    die "Failed to execute command: $cmd\n";
}

my $current_time = time();

# Read the JSON file
open my $fh, '<', '/tmp/pgs_dump.json' or die "Could not open file: $!";
my $json_text = do { local $/; <$fh> };
close $fh;

my $data = decode_json($json_text);

my %pg_osds;
my %pg_scrub_intervals;
my %pg_scrub_ids;
my %pg_deep_scrub_intervals;
my %pg_deep_scrub_ids;
my %deep_scrubbing;
my %scrubbing;
my %unclean;
my %unclean_deep;
my %osd;

my $max_scrub_interval = 0;
my $max_deep_scrub_interval = 0;

foreach my $pg (@{$data->{pg_stats}}) {
    my $pgid = $pg->{pgid};
    my $last_scrub_stamp = extract_datetime($pg->{last_scrub_stamp});
    my $last_deep_scrub_stamp = extract_datetime($pg->{last_deep_scrub_stamp});
    my $scrub_intervals = ceil(($current_time - str2time($last_scrub_stamp)) / (60 * 60 * 6));
    my $deep_scrub_intervals = ceil(($current_time - str2time($last_deep_scrub_stamp)) / (60 * 60 * 24));
    my $state = $pg->{state};
    my @acting = @{$pg->{acting}};

    foreach my $osd_id (@acting) {
        $pg_osds{$pgid} .= " $osd_id";
    }

    update_intervals_and_ids($state, $pgid, $scrub_intervals, $deep_scrub_intervals);
    update_max_intervals($scrub_intervals, $deep_scrub_intervals);
    update_osd_state($state, @acting);
}

say "Scrub Report:";
generate_report(\%pg_scrub_intervals, \%scrubbing, \%unclean, \%pg_scrub_ids, $max_scrub_interval, "6h");

say "";
say "Deep-Scrub Report:";
generate_report(\%pg_deep_scrub_intervals, \%deep_scrubbing, \%unclean_deep, \%pg_deep_scrub_ids, $max_deep_scrub_interval, "24h");

# Sammeln der OSDs aus der Zeile mit den meisten Intervallen
my %osds_to_highlight;

if (exists $pg_deep_scrub_ids{$max_deep_scrub_interval}) {
    my @pgs = split ' ', $pg_deep_scrub_ids{$max_deep_scrub_interval};
    foreach my $pg (@pgs) {
        my @osds = split ' ', $pg_osds{$pg};
        foreach my $osd_id (@osds) {
            $osds_to_highlight{$osd_id} = 1;
        }
    }
}

say "";
say "Current Deep Scrubs:";
get_current_deep_scrubs(\%osds_to_highlight);

say "";
say 'PGs marked with a * are not scrubbing because of busy OSDs.';
say 'OSDs highlighted in red block the longest waiting PG of deep-scrub.';

sub str2time {
    my ($str) = @_;
    my ($y, $m, $d, $H, $M, $S) = $str =~ /^(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)Z$/;
    return timegm($S, $M, $H, $d, $m - 1, $y);
}

sub extract_datetime {
    my ($datetime_str) = @_;
    if ($datetime_str =~ /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})/) {
        return $1 . "Z";
    }
    return "";
}

sub update_intervals_and_ids {
    my ($state, $pgid, $scrub_intervals, $deep_scrub_intervals) = @_;

    given ($state) {
        when ("active+clean") {
            $pg_scrub_intervals{$scrub_intervals}++;
            $pg_scrub_ids{$scrub_intervals} .= " $pgid";
            $pg_deep_scrub_intervals{$deep_scrub_intervals}++;
            $pg_deep_scrub_ids{$deep_scrub_intervals} .= " $pgid";
        }
        when (/scrubbing\+deep/) {
            $deep_scrubbing{$deep_scrub_intervals}++;
        }
        when (/scrubbing/) {
            $scrubbing{$scrub_intervals}++;
        }
        default {
            $unclean{$scrub_intervals}++;
            $unclean_deep{$deep_scrub_intervals}++;
            $pg_scrub_intervals{$scrub_intervals}++;
            $pg_scrub_ids{$scrub_intervals} .= " $pgid";
            $pg_deep_scrub_intervals{$deep_scrub_intervals}++;
            $pg_deep_scrub_ids{$deep_scrub_intervals} .= " $pgid";
        }
    }
}

sub update_max_intervals {
    my ($scrub_intervals, $deep_scrub_intervals) = @_;
    $max_scrub_interval = $scrub_intervals if $scrub_intervals > $max_scrub_interval;
    $max_deep_scrub_interval = $deep_scrub_intervals if $deep_scrub_intervals > $max_deep_scrub_interval;
}

sub update_osd_state {
    my ($state, @acting) = @_;
    if ($state =~ /scrubbing\+deep/ || $state =~ /scrubbing/ || $state !~ /active\+clean/) {
        foreach my $osd_id (@acting) {
            $osd{$osd_id} = "busy";
        }
    }
}

sub generate_report {
    my ($intervals_ref, $scrubbing_ref, $unclean_ref, $ids_ref, $max_interval, $interval_label) = @_;

    for my $interval (1 .. $max_interval) {
        next if (!exists $intervals_ref->{$interval} && !exists $scrubbing_ref->{$interval} && !exists $unclean_ref->{$interval});
        printf("%7d PGs not scrubbed since %2d intervals (%s)", $intervals_ref->{$interval} // 0, $interval, $interval_label);
        if (exists $scrubbing_ref->{$interval}) {
            printf(" scrubbing %d", $scrubbing_ref->{$interval});
        }
        if (exists $unclean_ref->{$interval}) {
            printf(" ⚠ %d", $unclean_ref->{$interval});
        }
        if (exists $intervals_ref->{$interval} && $intervals_ref->{$interval} <= 5) {
            my @pgs = split ' ', $ids_ref->{$interval};
            foreach my $pg (@pgs) {
                my $osds_busy = 0;
                my @osds = split ' ', $pg_osds{$pg};
                my @busy_osds;
                foreach my $osd_id (@osds) {
                    if (exists $osd{$osd_id} && $osd{$osd_id} eq "busy") {
                        $osds_busy = 1;
                        push @busy_osds, $osd_id;
                    }
                }
                printf(" %s%s", $pg, $osds_busy ? "*" : "");
                printf(" (%s)", join(", ", @osds)); # Immer alle OSDs anzeigen
            }
        }
        say "";
    }
}

sub get_current_deep_scrubs {
    my ($osds_to_highlight_ref) = @_;
    my %osds_to_highlight = %{$osds_to_highlight_ref};

    my $cmd = "ceph pg dump 2> /dev/null";
    open my $fh, '-|', $cmd or die "Could not run ceph pg dump: $!";

    my @deep_scrubs;
    while (<$fh>) {
        if (/scrubbing\+deep/ && /deep scrubbing for/) {
            my @fields = split;
            my $time_field = $fields[-3];
            my $osd_field = $fields[16];
            my $pg_field = $fields[0];

            if ($time_field =~ /^\d+s$/) {

                $time_field =~ s/s$//; # Remove the trailing 's'
                push @deep_scrubs, [$time_field, $osd_field, $pg_field];
            }
        }
    }
    close $fh;

    @deep_scrubs = sort { $a->[0] <=> $b->[0] } @deep_scrubs;
    printf("%s   %s      %s\n", "Since:", "OSDs:", "PG:");
    foreach my $scrub (@deep_scrubs) {
        my $osd_field = $scrub->[1];
        my $osd_list_str = $osd_field;
        $osd_list_str =~ s/[\[\]]//g;
        my @osd_ids = split /,/, $osd_list_str;
        @osd_ids = map { s/^\s+|\s+$//g; $_ } @osd_ids;
        my @highlighted_osd_ids;
        foreach my $osd_id (@osd_ids) {
            if (exists $osds_to_highlight{$osd_id}) {
                $osd_id = "\e[31m$osd_id\e[0m"; # Highlight in red
            }
            push @highlighted_osd_ids, $osd_id;
        }
        my $highlighted_osd_field = '[' . join(',', @highlighted_osd_ids) . ']';
        printf("%6ss  %s  %s\n", $scrub->[0], $highlighted_osd_field, $scrub->[2]);
    }
}
