using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using CommonFiles;

namespace DataQualitySummary
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            //MetaSummary Meta = new MetaSummary();

            //Meta.ExecuteSummary(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day - 1);

            //Meta.Notification();


            //RealtimeSummary Realtime = new RealtimeSummary();

            //Realtime.ExecuteSummary(@"Z:\Shu_Yang\MoDOT\MoDOT_RealtimeData\2012\06\10");

            //Realtime.Notification();

            SummaryNotification Summary = new SummaryNotification();

            Summary.Start();
        }
    }
}
