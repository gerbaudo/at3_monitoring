#!/bin/env python

"""
./plot_scratch_disk_usage.py tmp/at3-scratch-monitor/*.txt

Make plot of (usage / user) vs. time from the html excerpts

(gather data with the commands below)
# gerbaudo@at301 ~ $ tar czf - tmp/at3-queue-monitor/ | ssh lxplus.cern.ch "cat > foo.tgz"
# gerbaudo@lxplus ~ $ tar xzf ~/foo.tgz

davide.gerbaudo@gmail.com
Nov 2016
"""
from BeautifulSoup import BeautifulSoup
import array
import datetime
import re
import sys
import ROOT as R
R.gROOT.SetBatch(1)

def main():
    input_files = sys.argv[1:]
    print_names = True # False
    parsed_data = [parse_file(inf) for inf in input_files]
    all_table_names = sorted(list(set([table.name
                                       for data in parsed_data
                                       for table in data])))
    for disk_name in all_table_names:
        tables = [t for data in parsed_data for t in data if t.name==disk_name]
        print '%d tables for %s' % (len(tables), disk_name)
        tables.sort(key=lambda t: t.timestamp)
        tables = compress_usage(tables)
        graphs = {}
        min_usage = None
        max_usage = None
        min_time = None
        max_time = None
        for table in tables:
            date = R.TDatime(table.timestamp.strftime('%Y-%m-%d %H:%M:%S')).Convert()
            min_time = min_time if min_time and min_time<date else date
            max_time = max_time if max_time and max_time>date else date
            for entry in table.rows:
                gr = None
                if entry.user not in graphs:
                    gr = R.TGraph(0)
                    gr.SetName(disk_name+'_'+entry.user)
                    gr.SetTitle(entry.user)
                    graphs[entry.user] = gr
                else:
                    gr = graphs[entry.user]
                gr.SetPoint(gr.GetN(), date, entry.usage)
                min_usage = min_usage if min_usage and min_usage < entry.usage else entry.usage
                max_usage = max_usage if max_usage and max_usage > entry.usage else entry.usage
        can = R.TCanvas(disk_name, "Disk usage for %s" % disk_name)
        can._labels = []
        can.cd()
        pad_master = R.TH2D(disk_name+'_pm', can.GetTitle()+';Date [month/day]; Size [GB]', 100, min_time, max_time, 100, 0.5*min_usage, 1.1*max_usage)
        pad_master.GetXaxis().SetTimeDisplay(True)
        pad_master.GetXaxis().SetTimeFormat("%m/%d")
        pad_master.GetXaxis().SetTimeOffset(0,"gmt")
        pad_master.GetXaxis().CenterTitle()
        pad_master.GetYaxis().CenterTitle()
        pad_master.GetYaxis().SetTitleOffset(1.35*pad_master.GetYaxis().GetTitleOffset())
        pad_master.SetStats(False)
        pad_master.Draw('axis')
        for iGr, (u, gr) in enumerate(sorted(graphs.iteritems())):
            if u=='Total':
                gr.SetLineWidth(2*gr.GetLineWidth())
            gr.Draw('lp')
            if print_names:
                x_last = R.Double(0.0)
                y_last = R.Double(0.0)
                gr.GetPoint(gr.GetN()-1, x_last, y_last)
                # if 10.0*y_last>max_usage:
                if 10.0*y_last>0.0:
                    tex = R.TLatex(x_last, y_last, gr.GetName().replace(disk_name+'_', ''))
                    tex.SetTextFont(42)
                    tex.SetTextSize(0.5*tex.GetTextSize())
                    tex.Draw()
                    can._labels.append(tex)
        can.Update()
        can.SaveAs(disk_name.replace(' ', '_')+'.png')
        can.SaveAs(disk_name.replace(' ', '_')+'.root')
    

class Table(object):
    def __init__(self, name=None, timestamp=None, headers=[]):
        self.name = name
        self.timestamp = timestamp
        self.headers = headers
        self.rows = []
class Entry(object):
    def __init__(self, user=None, usage=None):
        self.user = user
        self.usage = usage

def parse_file(input_file, verbose=False):
    """extract info from a html with:
- 'Last update'
- <center> line with the name of the disk
- a '<table>' line with several <tr> elements
- the first row is the header
- the following ones are the usage per user
Based on http://stackoverflow.com/questions/11790535/extracting-data-from-html-table
    """
    timestamp = None
    tables = None
    with open(input_file) as inf:
        content = inf.read()
        html_lines = []
        for line in content.split('\n'):
            if not timestamp:
                match = re.search('Last update from (?P<date>\d{4}[-]?\d{1,2}[-]?\d{1,2})', line)
                timestamp = datetime.datetime.strptime(match.group('date'), '%Y-%m-%d') if match else None
            else:
                html_lines.append(line)
        # print 'html_lines ',html_lines
        # tree = ElementTree.fromstring(''.join(html_lines))
        # tables = tree.findall('table')
        soup = BeautifulSoup(''.join(html_lines))
        # print 'soup: ',soup
        sections = soup.findAll('center')
        tables = soup.findAll('table')

        if len(sections)!=len(tables):
            print "Warning: %d sections, %d tables, something might be parsed incorrectly" % (len(sections), len(tables))
        data = []
        for iSection, (section, table) in enumerate(zip(sections, tables)):
            rows = table.findAll('tr')
            headrow = rows[0]
            datarows = rows[1:]
            headers = [c.text for c in headrow.findAll('td')]
            column_name = headers.index('token')
            column_use  = headers.index('used (GB)')
            parsed_table = Table(name=section.text,
                                 timestamp=timestamp,
                                 headers=[headers[column_name], headers[column_use]])
            def clean_username(n_in):
                "transform '/DC=es/DC=irisgrid/O=ifae/CN=Jordi.Nadal' --> 'Jordi.Nadal'"
                n_out = n_in.split('=')[-1]
                return n_out
            for row in datarows:
                values = [c.text for c in row.findAll('td')]
                username = clean_username(values[column_name])
                usage = float(values[column_use])
                parsed_table.rows.append(Entry(user=username, usage=usage))
            data.append(parsed_table)
            if verbose:
                print "[%d] added table '%s'" %(iSection, parsed_table.name)
    return data
def compress_usage(tables):
    "loop over tables, and put in the 'other' entry all users that always have <5% of the total"
    # TODO
    return tables
            
if __name__=='__main__':
    main()
