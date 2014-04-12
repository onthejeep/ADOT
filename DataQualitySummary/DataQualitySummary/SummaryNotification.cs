using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using CommonFiles;

namespace DataQualitySummary
{
    class SummaryNotification
    {
        private MetaSummary _Meta;
        private RealtimeSummary _Realtime;

        private Timer _Timer;

        private ManageConfig _Config;


        public SummaryNotification()
        {
            _Meta = new MetaSummary();

            _Realtime = new RealtimeSummary();

            _Config = new ManageConfig(Application.StartupPath + "\\Data\\config.xml");

            _Config.Read();

            _Timer = new Timer();
            _Timer.Interval = 1000 * 60 *60 * 1;
            _Timer.Tick += new EventHandler(_Timer_Tick);
        }

        public void Start()
        {
            _Timer.Start();
        }

        void _Timer_Tick(object sender, EventArgs e)
        {
            if (DateTime.Now.Hour == 8)
            {
                DateTime Temp = DateTime.Now.AddDays(-1);

                //_Meta.ExecuteSummary(Temp.Year, Temp.Month, Temp.Day);

                //_Meta.Notification();

                //_Realtime.ExecuteSummary(string.Format("{0}\\{1:0000}\\{2:00}\\{3:00}", _Config.Config.Local, Temp.Year, Temp.Month, Temp.Day));
                _Realtime.ExecuteSummary(string.Format("{0}\\{1:0000}\\{2:00}\\{3:00}", _Config.Config.SharedDriveFolder, Temp.Year, Temp.Month, Temp.Day));

                _Realtime.Notification();
                _Realtime.Write2Log();
            }
        }


    }
}
