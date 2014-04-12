using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using CommonFiles;
using System.Data.SqlClient;

namespace DataQualitySummary
{
    class MetaSummary
    {
        public class StreetInfo
        {
            private string _Name;

            public string Name
            {
                get { return _Name; }
                set { _Name = value; }
            }
            private string _Count_DetectorsonStreet;

            public string Count_DetectorsonStreet
            {
                get { return _Count_DetectorsonStreet; }
                set { _Count_DetectorsonStreet = value; }
            }

            public StreetInfo(string name, string count)
            {
                _Name = name;
                _Count_DetectorsonStreet = count;
            }

            public override string ToString()
            {
                return string.Format("<tr><td>{0}</td><td>{1}</td></tr>", _Name, _Count_DetectorsonStreet);
            }
        }

        private int _Count_MetaFiles;

        public int Count_MetaFiles
        {
            get { return _Count_MetaFiles; }
            set { _Count_MetaFiles = value; }
        }
        private string _Time_MetaFile;

        public string Time_MetaFile
        {
            get { return _Time_MetaFile; }
            set { _Time_MetaFile = value; }
        }
        private int _Count_Detectors;

        public int Count_Detectors
        {
            get { return _Count_Detectors; }
            set { _Count_Detectors = value; }
        }
        private List<StreetInfo> _Street_Detectors;

        internal List<StreetInfo> Street_Detectors
        {
            get { return _Street_Detectors; }
            set { _Street_Detectors = value; }
        }

        public MetaSummary()
        {
            _Count_MetaFiles = 0;

            _Count_Detectors = 0;

            _Street_Detectors = new List<StreetInfo>(32);
        }

        public void Notification()
        {
            StringBuilder Streets = new StringBuilder();

            Streets.AppendLine(@"<table width='150px' border='1'><tr><th>StreetName</th><th>The number of detectors</th></tr>");

            if (_Street_Detectors==null)
            {
                AlterMailService_Singleton.GetInisitance().Alter.SendMessage("No Meta", "No Meta files yesterday!");
                return;
            }

            foreach (StreetInfo Street in _Street_Detectors)
            {
                Streets.AppendLine(Street.ToString());
            }

            Streets.AppendLine("</table>");

            string SendMessage = string.Format(
                    @"<b>Date and Time in Meta Files: </b>{0}<br>
<b>The number of Meta File: </b>{1}<br>
<b>The number of Detectors: </b>{2}<br>
<b>Info on Street and Detector: </b><br>
{3}<br>",
                    _Time_MetaFile,
                    _Count_MetaFiles,
                    _Count_Detectors,
                    Streets.ToString());

            string Subject = @"Meta Data Summary";

            AlterMailService_Singleton.GetInisitance().Alter.SendMessage(Subject,SendMessage);

        }

        public void ExecuteSummary(int year, int month, int day)
        {
            SqlConnection Conn = Connect2DB.GetInisitance();

            _Count_MetaFiles = 0;
            _Count_Detectors = 0;

            _Street_Detectors = new List<StreetInfo>(32);

            GetCount_MetaFiles(Conn, year, month, day);
            GetCount_Detectors(Conn);
            GetStreetInfo(Conn);
        }

        private void GetCount_MetaFiles(SqlConnection conn, int year, int month, int day)
        {
            SqlCommand SelectCommand = new SqlCommand();
            SelectCommand.Connection = conn;
            SelectCommand.CommandText = string.Format(@"select distinct([Date_Time]) 
 from [ModotRealtimeData].[dbo].[MetaData] 
 where datepart(year, Date_Time) = {0} 
 and datepart(month, Date_Time) = {1} 
 and datepart(day, Date_Time) = {2}", year, month, day);

            using (SqlDataReader Reader = SelectCommand.ExecuteReader())
            {
                while (Reader.Read())
                {
                    _Time_MetaFile = Reader[0].ToString();

                    _Count_MetaFiles++;
                }
            }
        }

        private void GetCount_Detectors(SqlConnection conn)
        {
            if (_Count_MetaFiles == 0)
            {
                _Count_Detectors = -1;
                return;
            }

            SqlCommand SelectCommand = new SqlCommand();
            SelectCommand.Connection = conn;
            SelectCommand.CommandText = string.Format(@"select [DetectorID] from [ModotRealtimeData].[dbo].[MetaData] where [Date_Time] = '{0}' group by DetectorID", _Time_MetaFile);

            using (SqlDataReader Reader = SelectCommand.ExecuteReader())
            {
                while (Reader.Read())
                {
                    _Count_Detectors++;
                }
            }
        }

        private void GetStreetInfo(SqlConnection conn)
        {
            if (_Count_Detectors == -1)
            {
                _Street_Detectors = null;
                return;
            }

            SqlCommand SelectCommand = new SqlCommand();
            SelectCommand.Connection = conn;
            SelectCommand.CommandText = string.Format(@"SELECT StreetName, COUNT(StreetName)
  FROM [ModotRealtimeData].[dbo].[MetaData]
  where [Date_Time] = '{0}'  group by StreetName", _Time_MetaFile);

            using (SqlDataReader Reader = SelectCommand.ExecuteReader())
            {
                while (Reader.Read())
                {
                    StreetInfo Temp = new StreetInfo(Reader[0].ToString(), Reader[1].ToString());

                    _Street_Detectors.Add(Temp);
                }
            }
        }
    }
}
