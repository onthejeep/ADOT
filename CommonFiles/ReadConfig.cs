using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Xml;

namespace CommonFiles
{
    /// <summary>
    /// Including configuration file content.
    /// </summary>
    public class ConfigData
    {
        public struct Database_ConfigData
        {
            public string Name;
            public string InitialCatalog;
            public string User;
        }

        public struct FTP_ConfigData
        {
            public string IP;
            public string Protocol;
            public string Port;
            public string User;
            public string Pwd;
            public string Folder;
            public string RealtimeDataFileName;
            public string MetaDataFileName;

            public string FilePath_RealtimeData
            {
                get
                {
                    return Folder + RealtimeDataFileName;
                }
            }

            public string FilePath_MetaData
            {
                get
                {
                    return Folder + MetaDataFileName;
                }
            }
        }

        public struct Local_ConfigData
        {
            public string Folder;
            public string UpdateFrequency_Realtime;
            public string UpdateFrequency_Meta;

            public string FormName;
        }

        private Database_ConfigData _Database;

        public Database_ConfigData Database
        {
            get { return _Database; }
            set { _Database = value; }
        }

        private FTP_ConfigData _FTP;

        public FTP_ConfigData FTP
        {
            get { return _FTP; }
        }

        private Local_ConfigData _Local;

        public Local_ConfigData Local
        {
            get { return _Local; }
        }

        private string _SharedDriveFolder;

        public string SharedDriveFolder
        {
            get { return _SharedDriveFolder; }
        }

        public ConfigData(string IP, string protocol, string port, string user, string pwd, string ftpFolder, string realtimeDataFileName, string metaDataFileName,
            string localFolder, string updateFrequency_Realtime, string updateFrequency_Meta, string formName,
            string sharedDriveFolder,
            string databaseName, string initialCatalog, string databaseUser)
        {
            _FTP.IP = IP;
            _FTP.Protocol = protocol;
            _FTP.Port = port;
            _FTP.User = user;
            _FTP.Pwd = pwd;
            _FTP.Folder = ftpFolder;
            _FTP.RealtimeDataFileName = realtimeDataFileName;
            _FTP.MetaDataFileName = metaDataFileName;

            _Local.Folder = localFolder;
            _Local.UpdateFrequency_Realtime = updateFrequency_Realtime;
            _Local.UpdateFrequency_Meta = updateFrequency_Meta;
            _Local.FormName = formName;

            _SharedDriveFolder = sharedDriveFolder;

            _Database.Name = databaseName;
            _Database.InitialCatalog = initialCatalog;
            _Database.User = databaseUser;
        }

       
    }

    public class ManageConfig
    {
        /// <summary>
        /// Relative Path, For example: data\config.xml
        /// </summary>
        private string _ConfigFileName;

        private ConfigData _Config;

        public ConfigData Config
        {
            get { return _Config; }
            set { _Config = value; }
        }

        /// <summary>
        /// For example: "new ManageConfig(@"Data\config.xml");"
        /// </summary>
        /// <param name="fileName"></param>
        public ManageConfig(string fileName)
        {
            _ConfigFileName = fileName;

            _Config = null;
        }

        /// <summary>
        /// Checking the validation of the full path of local folder used to store the real time raw data and meta data
        /// rules: 
        /// 1. if the folder exsits or the folder can be created successfully, the return value is true
        /// 2. if the folder does not exsit and cannot be created successfully, the return value is false
        /// </summary>
        /// <returns></returns>
        public bool CheckPathValidation()
        {
            if (Directory.Exists(_Config.Local.Folder))
            {
                return true;
            }
            else
            {
                try
                {
                    Directory.CreateDirectory(_Config.Local.Folder);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public void Read()
        {
            try
            {
                XmlDocument ConfigurationFile = new XmlDocument();
                ConfigurationFile.Load(_ConfigFileName);
                XmlElement Root = ConfigurationFile.DocumentElement;

                XmlNodeList IPNode = Root.SelectNodes("Config//FTP//IP");
                XmlNodeList ProtocolNode = Root.SelectNodes("Config//FTP//Protocol");
                XmlNodeList PortNode = Root.SelectNodes("Config//FTP//Port");
                XmlNodeList UserNode = Root.SelectNodes("Config//FTP//User");
                XmlNodeList PwdNode = Root.SelectNodes("Config//FTP//Pwd");
                XmlNodeList FTPFolderNode = Root.SelectNodes("Config//FTP//Folder");
                XmlNodeList RealtimeDataFileNameNode = Root.SelectNodes("Config//FTP//RealtimeData");
                XmlNodeList MetaDataFileNameNode = Root.SelectNodes("Config//FTP//MetaData");

                XmlNodeList LocalFolderNode = Root.SelectNodes("Config//Local//Folder");
                XmlNodeList UpdateNode_Realtime = Root.SelectNodes("Config//Local//UpdateFrequency_Realtime");
                XmlNodeList UpdateNode_Meta = Root.SelectNodes("Config//Local//UpdateFrequency_Meta");
                XmlNodeList FormName = Root.SelectNodes("Config//Local//FormName");

                XmlNodeList SharedDriveFolder = Root.SelectNodes("Config//SharedDrive//Folder");

                XmlNodeList DatabaseNameNode = Root.SelectNodes("Config//Database//Name");
                XmlNodeList InitialCatalogNode = Root.SelectNodes("Config//Database//InitialCatalog");
                XmlNodeList DBUserNode = Root.SelectNodes("Config//Database//User");

                _Config = new ConfigData(IPNode[0].InnerText, ProtocolNode[0].InnerText, PortNode[0].InnerText,
                    UserNode[0].InnerText, PwdNode[0].InnerText,
                    FTPFolderNode[0].InnerText, RealtimeDataFileNameNode[0].InnerText, MetaDataFileNameNode[0].InnerText,
                    LocalFolderNode[0].InnerText, UpdateNode_Realtime[0].InnerText, UpdateNode_Meta[0].InnerText, FormName[0].InnerText,
                    SharedDriveFolder[0].InnerText,
                    DatabaseNameNode[0].InnerText, InitialCatalogNode[0].InnerText, DBUserNode[0].InnerText);

            }
            catch (IOException ex)
            {
            }
        }

        /// <summary>
        /// Resevered Function
        /// </summary>
        /// <param name="configData"></param>
        public void Write(ConfigData configData)
        {
            //if (Directory.Exists("Data"))
            //{
            //    Directory.CreateDirectory("Data");
            //}

            //XmlDocument ConfigurationFile = new XmlDocument();

            //XmlNode Root = ConfigurationFile.CreateNode(XmlNodeType.Element, "realtime", "http://www.slu.edu");
            //XmlNode DocumentNode = ConfigurationFile.CreateNode(XmlNodeType.Element, "Config", null);
            //Root.AppendChild(DocumentNode);
            //ConfigurationFile.AppendChild(Root);


            //XmlNode MoDOTPathNode = ConfigurationFile.CreateNode(XmlNodeType.Element, "MoDOTPath", null);
            //XmlNode SLUPathNode = ConfigurationFile.CreateNode(XmlNodeType.Element, "SLUPath", null);
            //XmlNode UpdateNode = ConfigurationFile.CreateNode(XmlNodeType.Element, "UpdateFrequency", null);

            //MoDOTPathNode.InnerText = configData.MoDoTPath;
            //SLUPathNode.InnerText = configData.SLUPath;
            //UpdateNode.InnerText = configData.UpdateFrequency.ToString();

            //DocumentNode.AppendChild(MoDOTPathNode);
            //DocumentNode.AppendChild(SLUPathNode);
            //DocumentNode.AppendChild(UpdateNode);

            //XmlWriterSettings Settings = new XmlWriterSettings();
            //Settings.Indent = true;
            //Settings.IndentChars = ("   ");
            //UTF8Encoding XmlEncoding = new UTF8Encoding(false);
            //Settings.Encoding = XmlEncoding;

            //XmlWriter ConfigWriter = XmlWriter.Create(_ConfigFileName, Settings);

            //ConfigurationFile.WriteTo(ConfigWriter);

            //ConfigWriter.Close();
        }
    }
}
