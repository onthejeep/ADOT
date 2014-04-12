using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

using CommonFiles;

namespace DataQualitySummary
{
    public class Comparison_DateModified : IComparer<string>
    {
        #region IComparer<string> Members

        public int Compare(string current, string previous)
        {
            return current.CompareTo(previous);
        }

        #endregion
    }

    class RealtimeSummary
    {
        private string _DirectaryPath_Realtime;

        private DateTime _Time_RealtimeFile;

        private int _Count_Realtime;

        public int Count_Realtime
        {
            get { return _Count_Realtime; }
            set { _Count_Realtime = value; }
        }

        private int _Count_Missing;

        public int Count_Missing
        {
            get { return _Count_Missing; }
            set { _Count_Missing = value; }
        }

        public RealtimeSummary()
        {
            _Count_Realtime = 0;
        }

        public void Write2Log()
        {
            using (FileStream Fs = new FileStream("Log.txt", FileMode.Append, FileAccess.Write))
            {
                using (StreamWriter Sw = new StreamWriter(Fs))
                {
                    StringBuilder Sb = new StringBuilder();
                    Sb.Append(_Time_RealtimeFile.ToString() + "\t");
                    Sb.Append(_Count_Realtime + "\t");
                    Sb.Append(_Count_Missing + "\t");
                    Sw.Write(Sb.ToString());

                    Conditions Condi = Weather.GetCurrentConditions();
                    Condi.Output(Sw);  
                }
            }
        }

        public void Notification()
        {
            string SendMessage = string.Format(
                    @"<b>Date and Time: </b>{0}<br>
<b>The number of Realtime Files: </b>{1}<br>
<b>The number of Missing Files: </b>{2}<br>",
                    _Time_RealtimeFile.ToString(),
                    _Count_Realtime,
                    _Count_Missing);

            string Subject = @"Realtime Data Summary";

            AlterMailService_Singleton.GetInisitance().Alter.SendMessage(Subject, SendMessage);
        }

        public void ExecuteSummary(string directoryPath)
        {
            _Time_RealtimeFile = DateTime.Now.AddDays(-1);

            _Count_Realtime = 0;
            _Count_Missing = 0;

            _DirectaryPath_Realtime = directoryPath;
            GetFileList();
        }

        private void GetFileList()
        {
            IComparer<string> Compa = new Comparison_DateModified();

            string[] FileName = null;

            try
            {
                FileName = Directory.GetFiles(_DirectaryPath_Realtime, "*.xml");
            }
            catch (IOException ex)
            {
                _Count_Realtime = -1;
                _Count_Missing = -1;

                return;
            }

            if (FileName == null || FileName.Length == 0)
            {
                _Count_Realtime = -1;
                _Count_Missing = -1;

                return;
            }

            List<string> FileNameList = new List<string>(FileName.Length);
            FileNameList.AddRange(FileName);
            FileNameList.Sort(Compa);

            DateTime Previous = DateTime.MinValue;
            DateTime Current;

            double TimeDiff = 0;

            for (int i = 0; i < FileNameList.Count; i++)
            {
                if (Path.GetFileNameWithoutExtension(FileNameList[i]).Substring(0, 1) != "_")
                { 
                    DateTime Temp = ParseFileName(Path.GetFileNameWithoutExtension(FileNameList[i]));

                    _Count_Missing = Temp.Hour * 60 * 2 + Temp.Minute * 2;

                    break;
                }
            }

            for (int i = 0; i < FileNameList.Count; i++)
            {
                if (Path.GetFileNameWithoutExtension(FileNameList[i]).Substring(0, 1) != "_")
                {
                    _Count_Realtime++;

                    Current = ParseFileName(Path.GetFileNameWithoutExtension(FileNameList[i]));

                    TimeDiff = Difference_Datetime(Current, Previous);

                    Previous = new DateTime(Current.Ticks);

                    if (TimeDiff > 35)
                    {
                        _Count_Missing +=  ((int)TimeDiff / 30 - 1);
                    }
                }
            }
        }

        private DateTime ParseFileName(string fileName)
        {
            string[] Parts = fileName.Split('_');

            return new DateTime(int.Parse(Parts[0]), int.Parse(Parts[1].Substring(0,2)), int.Parse(Parts[1].Substring(2,2)),
            int.Parse(Parts[2].Substring(0,2)), int.Parse(Parts[2].Substring(2,2)), int.Parse(Parts[3].Substring(0,2)));
        }

        private double Difference_Datetime(DateTime current, DateTime pre)
        {
            if (pre == DateTime.MinValue || current == DateTime.MinValue)
            {
                return -1;
            }

            TimeSpan Ts = current - pre;

            return Ts.TotalSeconds;
        }

    }
}
