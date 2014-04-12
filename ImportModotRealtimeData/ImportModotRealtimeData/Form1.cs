using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Diagnostics;
using System.IO;
using System.Threading;

using CommonFiles;
using BasicImportData;

namespace ImportModotRealtimeData
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private FileSystemWatcher _FileMonitor;

        private ManageConfig _Config;

        private string _PreviousMonth = null;
        private string _CurrentMonth = null;

        private void Form1_Load(object sender, EventArgs e)
        {
            _Config = new ManageConfig(Application.StartupPath + "\\Data\\config.xml");
            _Config.Read();

            _FileMonitor = new FileSystemWatcher();
            _FileMonitor.Path = _Config.Config.SharedDriveFolder;
            _FileMonitor.IncludeSubdirectories = true;
            _FileMonitor.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName;
            _FileMonitor.Filter = "*.xml";

            _FileMonitor.Created += new FileSystemEventHandler(_FileMonitor_Created);
            _FileMonitor.Error += new ErrorEventHandler(_FileMonitor_Error);

            _FileMonitor.EnableRaisingEvents = true;
            
        }

        void _FileMonitor_Error(object sender, ErrorEventArgs e)
        {
            Exception Ex = e.GetException();

            Debug.WriteLine("_FileMonitor_Error" + Ex.Message);

            AlterMailService_Singleton.GetInisitance().Alter.SendMessage("FileMonitor_Error", Ex.Message);

            _FileMonitor = new FileSystemWatcher();
            _FileMonitor.Path = _Config.Config.SharedDriveFolder;
            _FileMonitor.IncludeSubdirectories = true;
            _FileMonitor.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName;
            _FileMonitor.Filter = "*.xml";

            _FileMonitor.Created += new FileSystemEventHandler(_FileMonitor_Created);
            _FileMonitor.Error += new ErrorEventHandler(_FileMonitor_Error);

            _FileMonitor.EnableRaisingEvents = true;
        }

        void _FileMonitor_Created(object sender, FileSystemEventArgs e)
        {
            Thread.Sleep(2000);

            if (Path.GetFileName(e.FullPath).Length < 6)
            {
                Debug.WriteLine(e.FullPath);
                Debug.WriteLine("Path.GetFileName(e.FullPath).Length < 6");
            }
            else
            {
                try
                {
                    if (Path.GetFileName(e.FullPath).Substring(0, 5) == "_Meta")
                    {
                        MetaData Meta = new MetaData(e.FullPath);

                        Meta.Write2DB();
                    }
                    else
                    {
                        string FileName = Path.GetFileName(e.FullPath);
                        string Year = FileName.Substring(0, 4);
                        _CurrentMonth = FileName.Substring(5, 2);

                        if (_CurrentMonth != _PreviousMonth)
                        {
                            if (!RealtimeData.TableExist(string.Format("{0}_{1}", Year, _CurrentMonth)))
                            {
                                RealtimeData.CreateNewTable(Year, _CurrentMonth);
                            }

                            _PreviousMonth = _CurrentMonth;
                        }

                        RealtimeData Realtime = new RealtimeData(e.FullPath);

                        Realtime.Write2DB();
                    }
                }
                catch (Exception ex)
                {
                    AlterMailService_Singleton.GetInisitance().Alter.SendMessage("FileMonitor_Created", ex.Message);
                }
            }
        }
    }
}
