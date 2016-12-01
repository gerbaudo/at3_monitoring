#!/bin/env python

"""

Take a bunch of files with jobs/user, fill a rrdtool database, and make plots.
Note: cannot get rddtool implementation to work; use root (hourly last week + daily)

Requires rrdtool:
# USE="python" emerge --jobs 2 -av rrdtool

Expects values at every hour.
Will discard readings at unusual times.
TODO Will fill gaps with values from previous readings.

davide.gerbaudo@gmail.com
Nov 2016
"""
import collections
import colorsys
import datetime
import glob
import os
import random
import re
import rrdtool
import subprocess
import time
import ROOT as R
R.gROOT.SetBatch(True)

def main():
    ""
    input_files = sorted(glob.glob('tmp/at3-queue-monitor/*.txt'))
    cmd = 'grep --no-filename " : " tmp/at3-queue-monitor/*.txt | awk \'{ print $1 }\' | sort | uniq'
    print getCommandOutput(cmd)['stdout'].split()
    usernames = sorted(getCommandOutput(cmd)['stdout'].split())
    print "Usernames: "+','.join(usernames)
    input_files = [Record(f) for f in input_files]
    input_files.sort(key=lambda r: r.timestamp)
    to_be_dropped = spurious_readings(input_files)
    print "dropping %d spurious readings" % len(to_be_dropped)
    input_files = [v for i,v in enumerate(input_files) if i not in frozenset(to_be_dropped)]

    rrd_filename = '/tmp/at3_queue.rdd'
    first_time = input_files[0].timestamp
    last_time = input_files[-1].timestamp
    last_week_end = last_time
    last_week_start = (datetime.datetime.fromtimestamp(time.mktime(last_week_end)) - datetime.timedelta(days=7)).timetuple()
    last_month_end = last_time
    last_month_start = (datetime.datetime.fromtimestamp(time.mktime(last_month_end)) - datetime.timedelta(days=30)).timetuple()
    number_of_days = abs((datetime.datetime.fromtimestamp(time.mktime(first_time)) -
                          datetime.datetime.fromtimestamp(time.mktime(last_time))).days)
    min_time = R.TDatime(time.strftime('%Y-%m-%d %H:%M:%S', last_week_start)).Convert()
    max_time = R.TDatime(time.strftime('%Y-%m-%d %H:%M:%S', last_week_end)).Convert()
    min_counts = 0
    max_counts = 0
    histos_last_week = {u : R.TH1F(u+'_last_week', u, 7*24,
                                   struct_time_2_root_time(last_week_start),
                                   struct_time_2_root_time(last_week_end))
                        for u in usernames}
    histos_daily = {u : R.TH1F(u+'_daily', u, number_of_days,
                                    struct_time_2_root_time(first_time),
                                    struct_time_2_root_time(last_time))
                         for u in usernames}
    day_average_counts = dict()
    for inpf in input_files:
        inpf.parse_file()
    timestamps = [inpf.timestamp for  inpf in input_files]
    daystamps = sorted(list(set([timestamp2daystamp(t) for t in timestamps])))
    daily_averages = compute_daily_averages(input_files)        
    # time_series = {u : pd.Series([(inpf.counts[u] if u in inpf.counts else 0) for inpf in input_files], index=timestamps)
    #                for u in usernames}
    for inpf in input_files:
        if inpf.timestamp < last_week_start:
            continue
        time_file = struct_time_2_root_time(inpf.timestamp)
        counts = inpf.counts
        bin_index = (histos_last_week.values()[0]).FindFixBin(float(time_file))
        min_counts = min([min_counts]+counts.values())
        max_counts = max([max_counts]+counts.values())
        for u, c in counts.iteritems():
            h = histos_last_week[u]
            h.SetBinContent(bin_index, float(c))
    for daily_average in daily_averages:
        time_day = struct_time_2_root_time(daily_average.timestamp)
        counts = daily_average.counts
        bin_index = (histos_daily.values()[0]).FindFixBin(float(time_day))
        # min_counts = min([min_counts]+counts.values())
        # max_counts = max([max_counts]+counts.values())
        for u, c in counts.iteritems():
            h = histos_daily[u]
            h.SetBinContent(bin_index, float(c))
    
    can_last_week = R.TCanvas('can_last_week', "jobs / user")
    can_last_week.cd()
    stack = R.THStack('jobs_users', 'jobs / user on at3')
    colors = generate_rgb_colors(usernames=usernames)
    for c, (u, h) in zip(colors, sorted(histos_last_week.iteritems()))[::-1]: # add to stack bot-top
        h.SetFillColor(R.TColor.GetColor(c[0], c[1], c[2])) # rgb to int
        stack.Add(h)
    pad_master = R.TH2D(can_last_week.GetName()+'_pm', can_last_week.GetTitle()+';Date [month/day]', 100,
                        struct_time_2_root_time(last_week_start),
                        struct_time_2_root_time(last_week_end), 10, min_counts, 1.1*max_counts)
    pad_master.GetXaxis().SetTimeDisplay(True)
    pad_master.GetXaxis().SetTimeFormat("%m/%d")
    pad_master.GetXaxis().SetTimeOffset(0,"gmt")
    pad_master.GetXaxis().CenterTitle()
    pad_master.GetYaxis().CenterTitle()
    pad_master.GetYaxis().SetTitleOffset(1.35*pad_master.GetYaxis().GetTitleOffset())
    pad_master.SetStats(False)
    pad_master.Draw('axis')
    stack.Draw('same')    
    leg_week = R.TLegend(0.9125,0.1,1.0,0.9)
    can_last_week._legend = leg_week
    leg_week.SetName('leg_week')
    leg_week.SetBorderSize(0)
    for (u, h) in sorted(histos_last_week.iteritems()):
        leg_week.AddEntry(h, u, 'f')
    leg_week.Draw()
    can_last_week.Update()
    can_last_week.SaveAs('at3_last_week.png')

    can_daily = R.TCanvas('can_daily', "jobs (day avg) / user")
    can_daily.cd()
    stack = R.THStack('jobs_users_daily', 'jobs (day avg) / user on at3')
    for c, (u, h) in zip(colors, sorted(histos_daily.iteritems()))[::-1]: # add to stack bot-top
        h.SetFillColor(R.TColor.GetColor(c[0], c[1], c[2])) # rgb to int
        stack.Add(h)
    pad_master = R.TH2D(can_daily.GetName()+'_pm', can_daily.GetTitle()+';Date [month/day]', 100,
                        struct_time_2_root_time(first_time),
                        struct_time_2_root_time(last_time), 10, min_counts, 1.1*max_counts)
    pad_master.GetXaxis().SetTimeDisplay(True)
    pad_master.GetXaxis().SetTimeFormat("%m/%d")
    pad_master.GetXaxis().SetTimeOffset(0,"gmt")
    pad_master.GetXaxis().CenterTitle()
    pad_master.GetYaxis().CenterTitle()
    pad_master.GetYaxis().SetTitleOffset(1.35*pad_master.GetYaxis().GetTitleOffset())
    pad_master.SetStats(False)
    pad_master.Draw('axis')
    stack.Draw('same')
    leg_daily = R.TLegend(0.9125,0.1,1.0,0.9)
    can_daily._legend = leg_daily
    leg_daily.SetName('leg_daily')
    leg_daily.SetBorderSize(0)
    for (u, h) in sorted(histos_daily.iteritems()):
        leg_daily.AddEntry(h, u, 'f')
    leg_daily.Draw()
    can_daily.Update()
    can_daily.SaveAs('at3_daily.png')

    
    # try with rrdtool (not working)    
    rrdtool.create(rrd_filename,
                   '--start', "%d" % (first_time - 1), #seconds since epoch
                   '--step', "%d" % (60*60), # 1h in s
                   ["DS:%s:GAUGE:%d:U:U" % (u, 2*60*60-1) for u in usernames], # DS:variable_name:DST:heartbeat:min:max, heartbeat = 2h-1s
                   'RRA:LAST:0.5:1:1024', # xff 0.5 (allow half missing)
                   # 'RRA:AVERAGE:0.5:24:7', # RRA:CF:xff:step:rows (weekly)
                   # 'RRA:AVERAGE:0.5:720:12'
    ) # (monthly)
    for inpf in input_files:
        counts = collections.defaultdict(int)
        with open(inpf.filename) as input_file:
            for line in input_file.readlines():
                if ':' not in line: continue
                user, jobs = line.split(':')
                user, jobs = user.strip(), jobs.strip().split()[0].strip()
                counts[user] = int(jobs)
        rrdtool.update(rrd_filename,
                       ':'.join(["%d" % time.mktime(inpf.timestamp)] +
                                ["%d" % counts[u] for u in usernames]))

    # var_defs = ["DEF:%s=%s:%s:AVERAGE"%(u, rrd_filename, u) for u in usernames]
    var_defs = ["DEF:%s=%s:%s:LAST"%(u, rrd_filename, u) for u in usernames]
    graph_defs = ["AREA:%s%s:%s" % (u, c, u) for u,c in zip(usernames, colors)[:1]] + ["STACK:%s%s:%s" % (u,c,u) for u,c in zip(usernames, colors)[1:]]
    print var_defs
    # print graph_defs
    rrdtool.graph(#rrd_filename,
        '--title', 'at3 queues: jobs per user (1h poll)',
        '--vertical-label', 'Jobs',
        '--font', 'AXIS:12',
        '--font', 'TITLE:16',
        '--font', 'UNIT:12',
        '--width', '800',
        '--height', '600',
        '--start', 'end-4m',
        '--end', '00:00',
        # DEF:x2=graph-example.rrd:x2:AVERAGE
        #     LINE2:x1#FF0000:x1
        #     LINE1:x2#0000FF:x2
        # AREA:x1#FF0000:x1
        # STACK:x2#0000FF:x2
        "jobs.png",
        var_defs+graph_defs
        # ["STACK:%s" % u for u in usernames[1:]]
    )
                      
class Record(object):
    """One reading of the jobs in the pbs queue
    Expect filenames to be named something like: 2016-02-21-23:00:03.txt
"""
    re_timestamp = re.compile(r'\d{4}-\d{2}-\d{2}-\d{2}:\d{2}:\d{2}')
    def __init__(self, filename):
        self.filename = filename
        self.timestamp = None
        self.counts = dict()
        bare_filename = os.path.splitext(os.path.basename(filename))[0]
        timestamp = Record.re_timestamp.findall(bare_filename)
        timestamp = timestamp[0] if timestamp else None
        if not timestamp:
            print "skipping file %s with invalid timestamp" % filename
        else:
            timestamp = time.strptime(timestamp, '%Y-%m-%d-%H:%M:%S')
            self.timestamp = timestamp
    def parse_file(self):
        with open(self.filename) as input_file:
            for line in input_file.readlines():
                if ':' not in line: continue
                user, jobs = line.split(':')
                user, jobs = user.strip(), jobs.strip().split()[0].strip()
                self.counts[user] = int(jobs)
        
class DailyAverage(object):
    "As a record, but w/out filename"
    def __init__(self, timestamp, counts):
        self.timestamp = timestamp
        self.counts = counts

def rgb_to_hex(red, green, blue):
    """Return color as #rrggbb for the given color values.
    http://stackoverflow.com/questions/214359/converting-hex-color-to-rgb-and-vice-versa
    """
    return '#%02x%02x%02x' % (red, green, blue)

def generate_colors(usernames=[]):
    "list from http://stackoverflow.com/questions/470690/how-to-automatically-generate-n-distinct-colors"
    colors = [
        '0xFFFFB300',
        '0xFF803E75',
        '0xFFFF6800',
        '0xFFA6BDD7',
        '0xFFC10020',
        '0xFFCEA262',
        '0xFF817066',
    # The following will not be good for people with defective color vision
        '0xFF007D34',
        '0xFFF6768E',
        '0xFF00538A',
        '0xFFFF7A5C',
        '0xFF53377A',
        '0xFFFF8E00',
        '0xFFB32851',
        '0xFFF4C800',
        '0xFF7F180D',
        '0xFF93AA00',
        '0xFF593315',
        '0xFFF13A13',
        '0xFF232C16']
    while len(usernames)>len(colors):
        print "%d users, %d colors, extending colors" %(len(users), len(colors))
        colors.extend(colors[:])
    return colors
def generate_rgb_colors(usernames=[]):
    # the implementation below would be better, but right now it doesn't work
    """from
    http://stackoverflow.com/questions/470690/how-to-automatically-generate-n-distinct-colors
    Assumes hue [0, 360), saturation [0, 100), lightness [0, 100)
    """
    num_colors = len(usernames)
    random.seed(1234)
    colors = []
    for i in range(0, 360, 360/num_colors):
        hue = i
        sat = 90 + random.random()*10
        lig = 50 + random.random()*10
        color = colorsys.hls_to_rgb(hue/360., lig/100., sat/100.)
        colors.append(color)
        # colors.append(rgb_to_hex(*color))
        # print 'color %d : %s (hls %d %d %d)' % (i, rgb_to_hex(*color), hue, lig, sat)
    return colors

def spurious_readings(readings=[]):
    "identify the readings that don't have at least one neighbor at 1h+/-10min"
    spurious = [] # indices
    fifty_min   = datetime.timedelta(minutes=50)
    seventy_min = datetime.timedelta(minutes=70)
    for iR, r in enumerate(readings):
        r_current = datetime.datetime(*r.timestamp[:6])
        r_before = datetime.datetime(*(readings[iR-1].timestamp)[:6]) if iR>0 else None
        r_after = datetime.datetime(*(readings[iR+1].timestamp)[:6]) if (iR+1+1)<len(readings) else None
        dt_before = (r_current - r_before) if r_before else None
        dt_after = (r_after - r_current) if r_after else None
        has_valid_before = dt_before and ((fifty_min < dt_before < seventy_min))
        has_valid_after = dt_after and (fifty_min < dt_after < seventy_min)
        is_valid = has_valid_before or has_valid_after
        is_spurious = not is_valid #(not has_valid_before) and (not has_valid_after)
        if is_spurious:
            print "dropping spurious entry at %s (before %s, after %s)" % (r_current.strftime("%Y-%m-%d %H:%M"),
                                                                           r_before.strftime("%Y-%m-%d %H:%M") if r_before else '--',
                                                                           r_after.strftime("%Y-%m-%d %H:%M") if r_after else '--')
            spurious.append(iR)
    return spurious

def struct_time_2_root_time(struct_time=None):
    "convert time.struct_time to root time"
    return R.TDatime(time.strftime('%Y-%m-%d %H:%M:%S', struct_time)).Convert()

def getCommandOutput(command, cwd=None):
    "lifted from supy (https://github.com/elaird/supy/blob/master/utils/io.py)"
    p = subprocess.Popen(command, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE, cwd = cwd)
    stdout,stderr = p.communicate()
    return {"stdout":stdout, "stderr":stderr, "returncode":p.returncode}

def timestamp2daystamp(t):
    'only keep Y M d'
    return datetime.datetime(t.tm_year, t.tm_mon, t.tm_mday).timetuple()

def compute_daily_averages(input_files):
    "take input records and build a list of daily averages"
    previous_day = input_files[0].timestamp
    counts_today = collections.defaultdict(int)
    records_per_day = 0
    daily_averages = []
    for inpf in input_files:
        this_day = inpf.timestamp
        same_day = abs((datetime.datetime.fromtimestamp(time.mktime(this_day)) -
                        datetime.datetime.fromtimestamp(time.mktime(previous_day))).days) < 1
        if not previous_day or same_day:
            for u, c in inpf.counts.iteritems():
                counts_today[u] += c
            records_per_day += 1
        else:
            daily_averages.append(DailyAverage(timestamp=this_day,
                                               counts={u : c/float(records_per_day) for u, c in counts_today.iteritems()}))
            counts_today = collections.defaultdict(int)
            records_per_day = 0
            previous_day = this_day
            for u, c in inpf.counts.iteritems():
                counts_today[u] += c
            records_per_day += 1
    return daily_averages


if __name__=='__main__':
    main()
