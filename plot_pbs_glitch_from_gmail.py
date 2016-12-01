#!/bin/env python

# Plot the occurences vs. time of notifications about cron job failures
#
# Based on the example here:
# http://stackoverflow.com/questions/13210737/get-only-new-emails-imaplib-and-python
# need to turn on
# https://www.google.com/settings/security/lesssecureapps
#
# davide.gerbaudo@cern.ch
#

import email, getpass, imaplib, os, pickle, pprint, re, time
from email.parser import HeaderParser
import ROOT as R
R.gROOT.SetBatch(True)

def main():
    today = time.strftime("%Y-%m-%d")
    cache_filename = "cron_notitication_cache_%s.pkl" % today
    notification_records = []
    if os.path.exists(cache_filename):
        notification_records = readFromPickle(cache_filename)
    else:
        user = raw_input("Enter your GMail username:")
        pwd = getpass.getpass("Enter your password: ")
        m = imaplib.IMAP4_SSL("imap.gmail.com") # connect
        m.login(user,pwd)
        parser = HeaderParser()
        m.select("ifae/operador", readonly=True) # I have a filter attaching the label ifae/operador to the Cron Daemon emails
        resp, items = m.search(None, "ALL")
        items = items[0].split() # getting the mails id
        for emailid in items:
            resp, hdr_data = m.fetch(emailid, "(BODY.PEEK[HEADER.FIELDS (SUBJECT DATE)])")
            hdr_txt = hdr_data[0][1]
            msg = parser.parsestr(hdr_txt)
            subj = msg['Subject']
            date = msg['Date']
            if ('at3-queue-monitor' in subj and 'Cron' in subj):
                rv, data = m.fetch(emailid, '(RFC822)')
                msg_from_string = email.message_from_string(data[0][1])
                body = ''
                if msg_from_string.is_multipart():
                    for payload in msg_from_string.get_payload():
                        body += payload.get_payload()
                else:
                    body += msg_from_string.get_payload()
                notification_records.append(FailureRecord(timestamp=time.mktime(email.utils.parsedate(date)),
                                                          body=body))
            else:
                continue
        dumpToPickle(filename=cache_filename, obj=notification_records)
    # collected dates, now plot
    types_of_records = sorted(list(set(r.body for r in notification_records)))
    print "%d cron messages of %d types" % (len(notification_records), len(types_of_records))
    print '\n'.join("[%d]-----\n%s\n----"%(it, tr) for it, tr in enumerate(types_of_records))
    notification_records = sorted(notification_records, key=lambda x: x.timestamp)
    if type(notification_records[0].timestamp) is float: # time.mktime gives a float, go back to struct_time
        for rec in notification_records:
            rec.timestamp = time.localtime(rec.timestamp)
    min_time = None
    max_time = None
    gr = R.TGraph(0)
    gr.SetName('g_pbs_cron_failures_'+today)
    cumulative_graphs = [R.TGraph(0) for tr in types_of_records]
    colors = [R.kOrange, R.kRed, R.kViolet, R.kCyan, R.kGreen]
    assert len(colors)>=len(cumulative_graphs)
    for iTr, tr in enumerate(types_of_records):
        cgr = cumulative_graphs[iTr]
        cgr.SetName('g_cumulative_pbs_cron_failures_type_'+str(iTr))
        cgr.SetTitle(tr)
        cgr.SetLineColor(colors[iTr])
    gr_cum = R.TGraph(0)
    gr_cum.SetName('g_cumulative_pbs_cron_failures_'+today)
    for iR, rec in enumerate(notification_records):
        nd = rec.timestamp
        date = R.TDatime(time.strftime('%Y-%m-%d %H:%M:%S', nd)).Convert()
        min_time = min_time if min_time and min_time<date else date
        max_time = max_time if max_time and max_time>date else date
        gr.SetPoint(gr.GetN(), date, 1)
        gr_cum.SetPoint(gr_cum.GetN(), date, iR)
        cum_graph = cumulative_graphs[types_of_records.index(rec.body)]
        cum_graph.SetPoint(cum_graph.GetN(), date, cum_graph.GetN())

    can = R.TCanvas('pbs_cron_failures_'+today, "PBS cron failures (every hr)")
    can.cd()
    can._labels = []
    pad_master = R.TH2D(can.GetName()+'_pm', can.GetTitle()+';Date [month/day]', 100, min_time, max_time, 10, 0.0, 1.1*len(notification_records))
    pad_master.GetXaxis().SetTimeDisplay(True)
    pad_master.GetXaxis().SetTimeFormat("%m/%d")
    pad_master.GetXaxis().SetTimeOffset(0,"gmt")
    pad_master.GetXaxis().CenterTitle()
    pad_master.GetYaxis().CenterTitle()
    pad_master.GetYaxis().SetTitleOffset(1.35*pad_master.GetYaxis().GetTitleOffset())
    pad_master.SetStats(False)
    pad_master.Draw('axis')
    gr.SetMarkerStyle(R.kFullCircle)
    gr.SetMarkerColor(R.kBlue)
    gr.Draw('p')
    gr_cum.SetLineColor(R.kBlue)
    gr_cum.Draw('l')
    for iCgr, cum_gr in enumerate(cumulative_graphs):
        cum_gr.Draw('l')
        x_last = R.Double(0.0)
        y_last = R.Double(0.0)
        cum_gr.GetPoint(cum_gr.GetN()-1, x_last, y_last)
        tex = R.TLatex(x_last, y_last, "[%d]"%iCgr)
        tex.SetTextFont(42)
        tex.SetTextSize(0.5*tex.GetTextSize())
        tex.Draw()
        can._labels.append(tex)
    date_wan_upgrade = R.TDatime('2016-09-14 12:00:00').Convert()
    arrow_wan_upgrade = R.TArrow(date_wan_upgrade, 0.2*len(notification_records), date_wan_upgrade, 0.10*len(notification_records))
    arrow_wan_upgrade.Draw()
    can._graphics = [arrow_wan_upgrade]
    can.Update()
    can.SaveAs(can.GetName()+'.png')

def dumpToPickle(filename='', obj=None) :
    output = open(filename, 'wb')
    pickle.dump(obj, output)
    output.close()

def readFromPickle(filename) :
    pkl_file = open(filename, 'rb')
    return pickle.load(pkl_file)

class FailureRecord(object):
    def __init__(self, timestamp, body):
        self.timestamp = timestamp
        self.body = body
if __name__=='__main__':
    main()
