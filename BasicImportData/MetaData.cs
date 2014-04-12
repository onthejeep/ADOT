using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Data.SqlClient;
using System.Data;

using CommonFiles;

namespace BasicImportData
{
    public class MetaData
    {
        private XmlDocument _MetaDoc;
        private Meta_Common _CommonInfo;

        public Meta_Common CommonInfo
        {
            get { return _CommonInfo; }
        }
        private List<Meta_Detector> _DetectorsInfo;

        public List<Meta_Detector> DetectorsInfo
        {
            get { return _DetectorsInfo; }
        }

        private SqlCommand _InsertCommand;

        public MetaData(string fileName)
        {
            _MetaDoc = new XmlDocument();
            _MetaDoc.Load(fileName);

            _CommonInfo = new Meta_Common();
            _DetectorsInfo = new List<Meta_Detector>(700);

            _InsertCommand = new SqlCommand("InsertMetaData");
            _InsertCommand.CommandType = System.Data.CommandType.StoredProcedure;

            _InsertCommand.Parameters.Add("Date_Time", SqlDbType.DateTime);
            _InsertCommand.Parameters.Add("Agency", SqlDbType.VarChar, 30);
            _InsertCommand.Parameters.Add("DetectorID", SqlDbType.VarChar, 20);
            _InsertCommand.Parameters.Add("DotID", SqlDbType.VarChar, 20);
            _InsertCommand.Parameters.Add("StreetName", SqlDbType.VarChar, 20);
            _InsertCommand.Parameters.Add("Direction", SqlDbType.VarChar, 20);
            _InsertCommand.Parameters.Add("CrossStreet", SqlDbType.VarChar, 50);
            _InsertCommand.Parameters.Add("Longitude", SqlDbType.Float);
            _InsertCommand.Parameters.Add("Latitude", SqlDbType.Float);
            _InsertCommand.Parameters.Add("AbsLogmile", SqlDbType.Float);
            _InsertCommand.Parameters.Add("LaneConfigurationList", SqlDbType.VarChar, 20);
            _InsertCommand.Parameters.Add("LaneTypeList", SqlDbType.VarChar, 30);

        }

        private DateTime ParseDatetime(string date, string time)
        {
            int Year = -11, Month = -11, Day = -11, Hour = -11, Minute = -11, Second = -11;

            try
            {
                Year = int.Parse(date.Substring(0, 4));
            }
            catch (Exception ex) { };

             try
            {Month = int.Parse(date.Substring(4, 2));}
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
            XmlElement Root = _MetaDoc.DocumentElement;

            XmlNodeList DateNode = Root.SelectNodes("date");
            XmlNodeList TimeNode = Root.SelectNodes("time");
            XmlNodeList AgencyNode = Root.SelectNodes("agency");

            _CommonInfo.ReceiveTime = ParseDatetime(DateNode[0].InnerText, TimeNode[0].InnerText);
            _CommonInfo.Agency = AgencyNode[0].InnerText;

            XmlNodeList DetectorList = Root.SelectNodes("detector");

            XmlNodeList DetectorIDNode, DotIDNode, StreetNameNode, DirectionNode, CrossStreet, LongitudeNode,
                LatitudeNode, AbslogmileNode, LaneConfigurationListNode, LaneTypeListNode;

            for (int i = 0; i < DetectorList.Count; i++)
            {
                DetectorIDNode = DetectorList[i].SelectNodes("detectorId");
                DotIDNode = DetectorList[i].SelectNodes("dotId");
                StreetNameNode = DetectorList[i].SelectNodes("linkLocation//crossStreets//onStreetName");
                DirectionNode = DetectorList[i].SelectNodes("linkLocation//crossStreets//onDirection");
                CrossStreet = DetectorList[i].SelectNodes("linkLocation//crossStreets//atCrossStreet");
                LongitudeNode = DetectorList[i].SelectNodes("linkLocation//crossStreets//onLongitude");
                LatitudeNode = DetectorList[i].SelectNodes("linkLocation//crossStreets//onLatitude");
                AbslogmileNode = DetectorList[i].SelectNodes("absLogmile");
                LaneConfigurationListNode = DetectorList[i].SelectNodes("laneConfigurationList");
                LaneTypeListNode = DetectorList[i].SelectNodes("laneTypeList");

                Meta_Detector Temp =  new Meta_Detector();

                Temp.DetectorID = DetectorIDNode[0].InnerText;
                Temp.DotID = DotIDNode[0].InnerText;
                Temp.StreetName = StreetNameNode[0].InnerText;
                Temp.Direction = DirectionNode[0].InnerText;
                Temp.CrossStreet = CrossStreet[0].InnerText;

                try { Temp.Longitude = double.Parse(LongitudeNode[0].InnerText); }
                catch (Exception ex) { }

                try { Temp.Latitude = double.Parse(LatitudeNode[0].InnerText); }
                catch (Exception ex) { }

                try { Temp.AbsLogmile = double.Parse(AbslogmileNode[0].InnerText); }
                catch (Exception ex) { }

                Temp.LaneConfigurationList = LaneConfigurationListNode[0].InnerText;
                Temp.LaneTypeList = LaneTypeListNode[0].InnerText;

                _DetectorsInfo.Add(Temp);
            }
        }

        public void Write2DB()
        {
            Read();

            for (int i = 0; i < _DetectorsInfo.Count; i++)
            {
                WriteSingleDetector2DB(_DetectorsInfo[i]);
            }
        }

        private void WriteSingleDetector2DB(Meta_Detector detector)
        {
            SqlConnection Conn = Connect2DB.GetInisitance();

            _InsertCommand.Connection = Conn;

            _InsertCommand.Parameters["Date_Time"].Value = _CommonInfo.ReceiveTime;
            _InsertCommand.Parameters["Agency"].Value = _CommonInfo.Agency;
            _InsertCommand.Parameters["DetectorID"].Value = detector.DetectorID;
            _InsertCommand.Parameters["DotID"].Value = detector.DotID;
            _InsertCommand.Parameters["StreetName"].Value = detector.StreetName;
            _InsertCommand.Parameters["Direction"].Value = detector.Direction;
            _InsertCommand.Parameters["CrossStreet"].Value = detector.CrossStreet;
            _InsertCommand.Parameters["Longitude"].Value = detector.Longitude;
            _InsertCommand.Parameters["Latitude"].Value = detector.Latitude;
            _InsertCommand.Parameters["AbsLogmile"].Value = detector.AbsLogmile;
            _InsertCommand.Parameters["LaneConfigurationList"].Value = detector.LaneConfigurationList;
            _InsertCommand.Parameters["LaneTypeList"].Value = detector.LaneTypeList;

            _InsertCommand.ExecuteNonQuery();

        }
    }

    public class Meta_Detector
    {
        public Meta_Detector()
        {
            _Longitude = -1;
            _Latitude = -1;
            _AbsLogmile = -1;
        }

        private string _DetectorID;

        public string DetectorID
        {
            get { return _DetectorID; }
            set { _DetectorID = value; }
        }
        private string _DotID;

        public string DotID
        {
            get { return _DotID; }
            set { _DotID = value; }
        }
        private string _StreetName;

        public string StreetName
        {
            get { return _StreetName; }
            set { _StreetName = value; }
        }
        private string _Direction;

        public string Direction
        {
            get { return _Direction; }
            set { _Direction = value; }
        }
        private string _CrossStreet;

        public string CrossStreet
        {
            get { return _CrossStreet; }
            set { _CrossStreet = value; }
        }
        private double _Longitude;

        public double Longitude
        {
            get { return _Longitude; }
            set { _Longitude = value; }
        }
        private double _Latitude;

        public double Latitude
        {
            get { return _Latitude; }
            set { _Latitude = value; }
        }
        private double _AbsLogmile;

        public double AbsLogmile
        {
            get { return _AbsLogmile; }
            set { _AbsLogmile = value; }
        }
        private string _LaneConfigurationList;

        public string LaneConfigurationList
        {
            get { return _LaneConfigurationList; }
            set { _LaneConfigurationList = value; }
        }
        private string _LaneTypeList;

        public string LaneTypeList
        {
            get { return _LaneTypeList; }
            set { _LaneTypeList = value; }
        }
    }
    public class Meta_Common
    {
        private DateTime _ReceiveTime;

        internal DateTime ReceiveTime
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
}
