using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Data.SqlClient;
using System.Data;

using CommonFiles;
using System.IO;

namespace BasicImportData
{
    public class RealtimeData
    {
        private StreamReader _RealtimeDoc;
        private Realtime_Common _CommonInfo;

        public Realtime_Common CommonInfo
        {
            get { return _CommonInfo; }
        }
        private List<Realtime_Detector> _Detectors;

        public List<Realtime_Detector> Detectors
        {
            get { return _Detectors; }
        }

        private string _Year = "1";
        private string _Month = "-1";
        private string _Date = null;
        private string _Hour = null;
        private string _Minute = null;

        private SqlCommand _InsertCommand;

        public RealtimeData(string fileName)
        {  
            _RealtimeDoc = File.OpenText(fileName);

            string FileName_NoPath = Path.GetFileNameWithoutExtension(fileName);
            string[] YearMonth = FileName_NoPath.Split('_');
            _Year = YearMonth[0];
            _Month = YearMonth[1].Substring(0,2);
            _Date = YearMonth[1].Substring(2,2);
            _Hour = YearMonth[2].Substring(0,2);
            _Minute = YearMonth[2].Substring(2,2);

            _CommonInfo = new Realtime_Common();
            _Detectors = new List<Realtime_Detector>(800);

            _CommonInfo.ReceiveTime = string.Format("{0}-{1}-{2} {3}:{4}:{5}", _Year, _Month, _Date, _Hour, _Minute, "00");
            _CommonInfo.Agency = "ADOT";

            _InsertCommand = new SqlCommand();

            _InsertCommand = new SqlCommand(string.Format("InsertRealtimeData"));
            _InsertCommand.CommandType = CommandType.StoredProcedure;

            _InsertCommand.Parameters.Add("year", SqlDbType.VarChar, 4);
            _InsertCommand.Parameters.Add("month", SqlDbType.VarChar, 4);
            _InsertCommand.Parameters.Add("det_stn_id", SqlDbType.VarChar, 8);
            _InsertCommand.Parameters.Add("upd_time", SqlDbType.VarChar, 25);

            _InsertCommand.Parameters.Add("avg_speed", SqlDbType.Float );
            _InsertCommand.Parameters.Add("avg_flow", SqlDbType.Float );
            _InsertCommand.Parameters.Add("avg_volume", SqlDbType.Float );
            _InsertCommand.Parameters.Add("avg_occupancy", SqlDbType.Float );

            _InsertCommand.Parameters.Add("flow_1", SqlDbType.Float);
            _InsertCommand.Parameters.Add("flow_2", SqlDbType.Float);
            _InsertCommand.Parameters.Add("flow_3", SqlDbType.Float);
            _InsertCommand.Parameters.Add("flow_4", SqlDbType.Float );
            _InsertCommand.Parameters.Add("flow_5", SqlDbType.Float );
            _InsertCommand.Parameters.Add("flow_6", SqlDbType.Float );
            _InsertCommand.Parameters.Add("flow_7", SqlDbType.Float );
            _InsertCommand.Parameters.Add("flow_8", SqlDbType.Float );

            _InsertCommand.Parameters.Add("speed_1", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_2", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_3", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_4", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_5", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_6", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_7", SqlDbType.Float );
            _InsertCommand.Parameters.Add("speed_8", SqlDbType.Float );

            _InsertCommand.Parameters.Add("volume_1", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_2", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_3", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_4", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_5", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_6", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_7", SqlDbType.Float );
            _InsertCommand.Parameters.Add("volume_8", SqlDbType.Float );

            _InsertCommand.Parameters.Add("occupancy_1", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_2", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_3", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_4", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_5", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_6", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_7", SqlDbType.Float );
            _InsertCommand.Parameters.Add("occupancy_8", SqlDbType.Float );
        }

        public static void CreateNewTable(string year, string month)
        {
            SqlCommand CreateTableCommand = new SqlCommand("CreateRealtimeTable");
            CreateTableCommand.CommandType = System.Data.CommandType.StoredProcedure;
            CreateTableCommand.Parameters.Add("year", SqlDbType.VarChar, 4);
            CreateTableCommand.Parameters.Add("month", SqlDbType.VarChar, 4);

            CreateTableCommand.Connection = Connect2DB.GetInisitance();

            CreateTableCommand.Parameters["year"].Value = year;
            CreateTableCommand.Parameters["month"].Value = month;

            try
            {
                CreateTableCommand.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                AlterMailService_Singleton.GetInisitance().Alter.SendMessage("CreateNewTable",  ex.Message);
            }
        }

        private DateTime ParseDatetime(string date, string time)
        {
            int Year = -11, Month = -11, Day = -11, Hour = -11, Minute = -11, Second = -11;

            try
            {
                Year = int.Parse(date.Substring(0, 4));

                _Year = date.Substring(0, 4);
            }
            catch (Exception ex) { };

             try
            {Month = int.Parse(date.Substring(4, 2));
            _Month = date.Substring(4, 2);
            }
            catch (Exception ex) { };
             try
            {Day = int.Parse(date.Substring(6, 2));}
            catch (Exception ex) { };

             try
            {Hour = int.Parse(time.Substring(0, 2));}
            catch (Exception ex) { };
             try
            {Minute = int.Parse(time.Substring(2, 2));}
            catch (Exception ex) { };
             try
             { Second = int.Parse(time.Substring(4, 2)); }
             catch (Exception ex) { };

            DateTime R = new DateTime(1,1,1,1,1,1);

            try
            {R= new DateTime(Year, Month, Day, Hour, Minute, Second);
            }catch (Exception ex) { };

            return R;
        }

        private void Read()
        {
            string Line = _RealtimeDoc.ReadLine();
            Line = _RealtimeDoc.ReadLine();

            string EachLine = null;

            char[] Delimiters = new char[] { ' ', '\t' };

            while (!_RealtimeDoc.EndOfStream)
            {
                EachLine = _RealtimeDoc.ReadLine();
                string[] Units = EachLine.Split(Delimiters, StringSplitOptions.RemoveEmptyEntries);

                Realtime_Detector Temp = new Realtime_Detector();
                Temp.DetectorID = Units[0];
                Temp.Avg_Speed = Units[1];
                Temp.Avg_Flow = Units[2];
                Temp.Avg_Volume = Units[3];
                Temp.Avg_Occupancy = Units[4];

                for (int i = 0; i < Temp.Lanes.Count; i++)
                {
                    Temp.Lanes[i].Flow = Units[5 + i];
                }

                for (int i = 0; i < Temp.Lanes.Count; i++)
                {
                    Temp.Lanes[i].Speed = Units[13 + i];
                }

                for (int i = 0; i < Temp.Lanes.Count; i++)
                {
                    Temp.Lanes[i].Volume = Units[21 + i];
                }

                for (int i = 0; i < Temp.Lanes.Count; i++)
                {
                    Temp.Lanes[i].Occupancy = Units[29 + i];
                }

                _Detectors.Add(Temp);
            }

            _RealtimeDoc.Close();
        }

        public void Write2DB()
        {
            Read();
            
            SqlConnection Conn = Connect2DB.GetInisitance();

            SqlTransaction Trans = Conn.BeginTransaction();

            for (int i = 0; i < _Detectors.Count; i++)
            {
                WriteSingleDetector2DB(_Detectors[i], Conn, Trans);
            }

            Trans.Commit();
        }

        private void WriteSingleDetector2DB(Realtime_Detector detector, SqlConnection conn, SqlTransaction trans)
        {
            _InsertCommand.Connection = conn;
            _InsertCommand.Transaction = trans;
            _InsertCommand.Parameters["year"].Value = _Year;
            _InsertCommand.Parameters["month"].Value = _Month;
            _InsertCommand.Parameters["det_stn_id"].Value = detector.DetectorID;
            _InsertCommand.Parameters["upd_time"].Value = _CommonInfo.ReceiveTime;

            _InsertCommand.Parameters["avg_speed"].Value = detector.Avg_Speed;
            _InsertCommand.Parameters["avg_flow"].Value = detector.Avg_Flow;
            _InsertCommand.Parameters["avg_volume"].Value = detector.Avg_Volume;
            _InsertCommand.Parameters["avg_occupancy"].Value = detector.Avg_Occupancy;

            string ParameterName = string.Empty;

            for (int i = 0; i < 8; i++)
            {
                ParameterName = string.Format("flow_{0}", i + 1);
                _InsertCommand.Parameters[ParameterName].Value = detector.Lanes[i].Flow;
            }

            for (int i = 0; i < 8; i++)
            {
                ParameterName = string.Format("speed_{0}", i + 1);
                _InsertCommand.Parameters[ParameterName].Value = detector.Lanes[i].Speed;
            }

            for (int i = 0; i < 8; i++)
            {
                ParameterName = string.Format("volume_{0}", i + 1);
                _InsertCommand.Parameters[ParameterName].Value = detector.Lanes[i].Volume;
            }

            for (int i = 0; i < 8; i++)
            {
                ParameterName = string.Format("occupancy_{0}", i + 1);
                _InsertCommand.Parameters[ParameterName].Value = detector.Lanes[i].Occupancy;
            }
            
            _InsertCommand.ExecuteNonQuery();

        }

        public static bool TableExist(string tableName)
        {
            SqlCommand SelectCommand = new SqlCommand();
            SelectCommand.CommandText = string.Format("select * from sys.Tables where name = '{0}'", tableName);
            SelectCommand.Connection = Connect2DB.GetInisitance();

            bool RowEffect = true;

            using (SqlDataReader Reader = SelectCommand.ExecuteReader())
            {
                RowEffect = Reader.Read();
            }

            return RowEffect;
        }
    }

    public class Realtime_Common
    {
        private string _ReceiveTime;

        internal string ReceiveTime
        {
            get { return _ReceiveTime; }
            set { _ReceiveTime = value; }
        }
        private string _Agency;

        public string Agency
        {
            get { return _Agency; }
            set { _Agency = value; }
        }
    }

    public class Realtime_Detector
    {
        private string _DetectorID;

        public string DetectorID
        {
            get { return _DetectorID; }
            set { _DetectorID = value; }
        }

        private string _Avg_Speed;

        public string Avg_Speed
        {
            get { return _Avg_Speed; }
            set { _Avg_Speed = value; }
        }
        private string _Avg_Flow;

        public string Avg_Flow
        {
            get { return _Avg_Flow; }
            set { _Avg_Flow = value; }
        }
        private string _Avg_Volume;

        public string Avg_Volume
        {
            get { return _Avg_Volume; }
            set { _Avg_Volume = value; }
        }
        private string _Avg_Occupancy;

        public string Avg_Occupancy
        {
            get { return _Avg_Occupancy; }
            set { _Avg_Occupancy = value; }
        }

        private List<Realtime_Lane> _Lanes;

        public List<Realtime_Lane> Lanes
        {
            get { return _Lanes; }
            set { _Lanes = value; }
        }

        public Realtime_Detector()
        {
            _DetectorID = null;
            _Lanes = new List<Realtime_Lane>(8);

            for (int i = 0; i < 8; i++)
            {
                _Lanes.Add(new Realtime_Lane());
            }
        }

        public class Realtime_Lane
        {
            public Realtime_Lane()
            {
                _Flow = null;
                _Occupancy = null;
            }

            private string _Speed;

            public string Speed
            {
                get { return _Speed; }
                set { _Speed = value; }
            }

            private string _Flow;

            public string Flow
            {
                get { return _Flow; }
                set { _Flow = value; }
            }

            private string _Volume;

            public string Volume
            {
                get { return _Volume; }
                set { _Volume = value; }
            }

            private string _Occupancy;

            public string Occupancy
            {
                get { return _Occupancy; }
                set { _Occupancy = value; }
            }
            
        }

    }

    

}
