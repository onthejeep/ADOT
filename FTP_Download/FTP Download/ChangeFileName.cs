using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography.X509Certificates;

using CommonFiles;
using System.Security;
using System.Net.Security;

namespace FTP_Download
{
    /// <summary>
    /// Using 2 indicator to check whether two files are the same
    /// 1. Timestamp
    /// 2. Filesize
    /// </summary>
    class ComparisonFilesStruct
    {
        private string _DataString;

        public string DataString
        {
            get { return _DataString; }
            set { _DataString = value; }
        }
        private string _TimeString;

        public string TimeString
        {
            get { return _TimeString; }
            set { _TimeString = value; }
        }
        private long _FileSize;

        public long FileSize
        {
            get { return _FileSize; }
            set { _FileSize = value; }
        }
        //private DateTime _ModifiedTime;

        //public DateTime ModifiedTime
        //{
        //    get { return _ModifiedTime; }
        //    set { _ModifiedTime = value; }
        //}

        public static void AssignCurrent2Previous(ref ComparisonFilesStruct previous,
            ref ComparisonFilesStruct current)
        {
            previous.DataString = current.DataString;
            previous.TimeString = current.TimeString;
            previous.FileSize = current.FileSize;
            //previous.ModifiedTime = current.ModifiedTime;
        }

        /// <summary>
        /// Comparing whether two files are the same
        /// </summary>
        /// <param name="previous"></param>
        /// <param name="current"></param>
        /// <returns></returns>
        public static bool Compare(ComparisonFilesStruct previous, ComparisonFilesStruct current)
        {
            if (previous == null)
            {
                return false;
            }

            if (
                (previous.DataString != current.DataString) 
                || (previous.TimeString != current.TimeString) 
                || (previous.FileSize != current.FileSize) 
                //|| (previous.ModifiedTime != current.ModifiedTime)
                )
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }

    /// <summary>
    /// The download strategies:
    /// 1. connect to the FTP site
    /// 2. transfer the desired files from FTP site
    /// 3. store the downloaded files into a temprary file, say ""C:\modotrealtime_temp.tmp"
    /// 4. copy the temporary files to specified location in local computers, 
    ///    and change the file names according to the information of date and time in the files
    /// </summary>
    class ChangeFileName
    {
        /// <summary>
        /// the path of the temporary file
        /// </summary>
        private string _TempDownloadFile;

        /// <summary>
        /// Full Path, including Directory Path and file name
        /// Basically, _DestinationFile_Local = _CurrentFolder_Local + _DestinationFileName
        /// </summary>
        private string _DestinationFile_Local;

        /// <summary>
        /// Full Path, including Directory Path and file name
        /// Basically, _DestinationFile_SharedDrive = _CurrentFolder_SharedDrive + _DestinationFileName
        /// </summary>
        private string _DestinationFile_SharedDrive;

        /// <summary>
        /// this parameter is used to distinguish the names of realtime and meta
        /// 1. The name of the realtime file is following the style "{year}_{monthday}_{hourminute}_{second}.xml"
        /// 2. The name of the Meta file is following the style "_Meta_{year}_{monthday}_{hourminute}_{second}.xml"
        /// </summary>
        private bool _IsMeta;

        /// <summary>
        /// This parameter is defined by MoDOT.
        /// Temporarily, the name of original realtime file is "DetectorRealtimeData.xml"
        /// and the name of original meta file is "DetectorMetaData.xml"
        /// </summary>
        private string _SourceFileName;

        /// <summary>
        /// only File Name without the folder path
        /// </summary>
        private string _DestinationFileName;

        /// <summary>
        /// Decriptoin: "\\Year\\Month\\Day", for the local storage
        /// </summary>
        private string _CurrentFolder_Local;

        /// <summary>
        /// Decriptoin: "\\Year\\Month\\Day", for the remote storage
        /// </summary>
        private string _CurrentFolder_SharedDrive;

        private ComparisonFilesStruct _PreviousFile;
        private ComparisonFilesStruct _CurrentFile;

        private ManageConfig _Config;

        private int _Temp_Length = 2048;
        private Byte[] _Temp_Buffer;

        private int _RepeatFileCount = 0;
        private int _RepeatFileCount_Thesh;

        //private Uri _FileUri;
        //private FtpWebRequest _ReqFTP;

        public ChangeFileName(ManageConfig config, string tempStoreFile, string sourceFile, bool isMeta)
        {
            _Config = config;

            _PreviousFile = new ComparisonFilesStruct();
            _CurrentFile = new ComparisonFilesStruct();

            _TempDownloadFile = tempStoreFile;
            _SourceFileName = sourceFile;

            _IsMeta = isMeta;

            _Temp_Buffer = new Byte[_Temp_Length];           
        }

        /// <summary>
        /// 1. Create corresponding Month and Day Folder
        /// 2. Assign timeStamp to _DestinationFileName
        /// 3. Set Comparison info
        /// </summary>
        private void OperationByTimeStamp()
        {
            string DateString;
            string TimeString;

            #region Get TimeStamp from XML # not used for ADOT

            //using (XmlReader Reader = XmlReader.Create(_TempDownloadFile))
            //{
            //    XmlDocument Doc = new XmlDocument();

            //    Doc.Load(Reader);

            //    XmlElement Root = Doc.DocumentElement;

            //    XmlNodeList DateNode = Root.SelectNodes("//date");
            //    XmlNodeList TimeNode = Root.SelectNodes("//time");

            //    DateString = DateNode[0].InnerText;
            //    TimeString = TimeNode[0].InnerText;
            //}

            #endregion

            #region Setting "Current File" info # not used for ADOT
            //FileInfo SourceXmlFileInfo;

            //SourceXmlFileInfo = new FileInfo(_TempDownloadFile);

            //_CurrentFile.DataString = DateString;
            //_CurrentFile.TimeString = TimeString;
            //_CurrentFile.FileSize = SourceXmlFileInfo.Length;
            //_CurrentFile.ModifiedTime = SourceXmlFileInfo.LastWriteTime;
            #endregion

            //Get the folder name from TimeStamp
            DateTime Current = DateTime.Now;

            string YearFolderName = string.Format("{0:0000}", Current.Year);
            string MonthFolderName = string.Format("{0:00}", Current.Month);
            string DayFolderName = string.Format("{0:00}", Current.Day);

            string Hour = string.Format("{0:00}", Current.Hour);
            string Minute = string.Format("{0:00}", Current.Minute);
            string Second = string.Format("{0:00}", Current.Second);


            //If the folder does not exsit, create it (by year and month)
            //Example: C:\Realtime\ + 2009\12\23
            _CurrentFolder_Local = string.Format("{0}\\{1}\\{2}\\{3}",
                _Config.Config.Local.Folder, YearFolderName, MonthFolderName, DayFolderName);

            _CurrentFolder_SharedDrive = string.Format("{0}\\{1}\\{2}\\{3}",
                _Config.Config.SharedDriveFolder, YearFolderName, MonthFolderName, DayFolderName);

            if (!Directory.Exists(_CurrentFolder_Local))
            {
                Directory.CreateDirectory(_CurrentFolder_Local);
            }

            if (_Config.Config.SharedDriveFolder != "none")
            {
                if (!Directory.Exists(_CurrentFolder_SharedDrive))
                {
                    Directory.CreateDirectory(_CurrentFolder_SharedDrive);
                }
            }

            //Assign timeStamp to _DestinationFileName
            _DestinationFileName = string.Format("{0}_{1}{2}_{3}{4}_{5}.xml", YearFolderName, MonthFolderName,
                DayFolderName, Hour, Minute, Second);

            //In this function, only the values of file names are assigned
            //the combination of _CurrentFolder and _DestinationFileName is not required
            //the step of combination will be taken in the function of "CopyFile()"
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName">the temporary file</param>
        private bool TransferFile_FromFTP(string fileName)
        {
            try
            {
                #region using FtpWebRequest

                Uri FileUri = new Uri("ftp://" + _Config.Config.FTP.IP + "/" + _SourceFileName);

                if (FileUri.Scheme != Uri.UriSchemeFtp)
                {
                    return false;
                }

                FtpWebRequest ReqFTP = (FtpWebRequest)WebRequest.Create(FileUri);

                //ReqFTP.Credentials = new NetworkCredential(_Config.Config.FTP.User, _Config.Config.FTP.Pwd);
                ReqFTP.KeepAlive = false;
                ReqFTP.Method = WebRequestMethods.Ftp.DownloadFile;
                ReqFTP.UseBinary = true;
                ReqFTP.Proxy = null;
                ReqFTP.UsePassive = true;

                using (FtpWebResponse Response = (FtpWebResponse)ReqFTP.GetResponse())
                {
                    using (Stream ResponseStream = Response.GetResponseStream())
                    {
                        using (FileStream WriteStream = new FileStream(fileName, FileMode.Create))
                        {
                            int BytesRead = ResponseStream.Read(_Temp_Buffer, 0, _Temp_Length);

                            while (BytesRead > 0)
                            {
                                WriteStream.Write(_Temp_Buffer, 0, BytesRead);
                                BytesRead = ResponseStream.Read(_Temp_Buffer, 0, _Temp_Length);
                            }

                            WriteStream.Flush();
                        }
                    }
                }
                #endregion

                return true;
            }
            catch (WebException ex)
            {
                Debug.WriteLine("cannot connect to the FTP server");

                return false;
                //This exception does always happen, due to the new features of .net 4.0
                #region
                //string SendMessage = string.Format(
                //   "<b>TargetSite:</b>{0}<br><b>StackTrace:</b>{1}<br><b>Message:</b>{2}",
                //   ex.TargetSite,
                //   ex.StackTrace,
                //   ex.Message);

                //string Subject = ex.Message.Substring(0, 50);

                //AlterMailService_Singleton.GetInisitance().Alter.SendMessage(Subject, SendMessage);
                #endregion
            }
            catch (Exception ex)
            {
                string SendMessage = string.Format(
                   "<b>TargetSite:</b>{0}<br><b>StackTrace:</b>{1}<br><b>Message:</b>{2}",
                   ex.TargetSite,
                   ex.StackTrace,
                   ex.Message);

                string Subject = "TransferFile_FromFTP";// ex.Message.Substring(0, 50);

                AlterMailService_Singleton.GetInisitance().Alter.SendMessage(Subject, SendMessage);

                return false;
            }
        }

        public void CopyFile()
        {
            if (!_Config.CheckPathValidation())
            {
                return;
            }

            if (!TransferFile_FromFTP(_TempDownloadFile))
            {
                return;
            }

            FileInfo TempFileInfo = new FileInfo(_TempDownloadFile);
            if (TempFileInfo.Length == 0)
            {
                return;
            }


            OperationByTimeStamp();
            
            //If the two files are the same, jump out of this function
            //if (ComparisonFilesStruct.Compare(_PreviousFile, _CurrentFile) == true)
            //{
            //    //AlterMailService_Singleton.GetInisitance().Alter.SendMessage("MoDOT", "The real time file has not been changed.");
            //    return;
            //}

            if (_IsMeta)
            {
                _DestinationFile_Local = string.Format("{0}\\_Meta_{1}",
                _CurrentFolder_Local,
                _DestinationFileName);

                //_DestinationFile_SharedDrive = string.Format("{0}\\_Meta_{1}",
                //_CurrentFolder_SharedDrive,
                //_DestinationFileName);
            }
            else
            {
                _DestinationFile_Local = string.Format("{0}\\{1}",
                               _CurrentFolder_Local,
                               _DestinationFileName);

                //_DestinationFile_SharedDrive = string.Format("{0}\\{1}",
                //               _CurrentFolder_SharedDrive,
                //               _DestinationFileName);
            }

            //copy temporary file to local storage
            try
            {
                if (!File.Exists(_DestinationFile_Local))
                {
                    File.Copy(_TempDownloadFile, _DestinationFile_Local);

                    //ComparisonFilesStruct.AssignCurrent2Previous(ref _PreviousFile, ref _CurrentFile);
                }
                else
                {
                    string SendMessage = "copy temporary file to local storage";

                    string Subject = "Have not updated for 10 minutes";

                    AlterMailService_Singleton.GetInisitance().Alter.SendMessage(Subject, SendMessage);                    
                }
            }
            catch (IOException ex)
            {
                string SendMessage = string.Format(
                    "<b>TargetSite:</b>{0}<br><b>StackTrace:</b>{1}<br><b>Message:</b>{2}",
                    ex.TargetSite,
                    ex.StackTrace,
                    ex.Message);

                string Subject = "CopyFile";// ex.Message.Substring(0, 50);

                AlterMailService_Singleton.GetInisitance().Alter.SendMessage(Subject, SendMessage);
            }

            //copy temporary file to remote storage
            //try
            //{
            //    if (_Config.Config.SharedDriveFolder != "none")
            //    {
            //        if (!File.Exists(_DestinationFile_SharedDrive))
            //        {
            //            File.Copy(_TempDownloadFile, _DestinationFile_SharedDrive);
            //        }
            //    }
                
            //}
            //catch (IOException ex)
            //{
            //}
        }
    }
}
