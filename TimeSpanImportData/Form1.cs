using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using System.Diagnostics;

using CommonFiles;
using BasicImportData;

namespace TimeSpanImportData
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private ManageConfig _Config;

        private string _PreviousMonth = null;
        private string _CurrentMonth = null;

        private Timer _Timer;

        private void Import(DateTime startDate, DateTime endDate)
        {
            DateTime Temp_StartDate = new DateTime(startDate.Ticks);

            string FolderPath;

            while (Temp_StartDate <= endDate)
            {
                FolderPath = string.Format("{0}\\{1:0000}\\{2:00}\\{3:00}", _Config.Config.SharedDriveFolder, Temp_StartDate.Year, Temp_StartDate.Month, Temp_StartDate.Day);

                string[] Files = GetFileList(FolderPath);

                if (Files == null)
                {
                    Temp_StartDate = Temp_StartDate.AddDays(1);
                    continue;
                }

                for (int i = 0; i < Files.Length; i++)
                {
                    try
                    {
                        if (Path.GetFileName(Files[i]).Substring(0, 5) == "_Meta")
                        {
                            MetaData Meta = new MetaData(Files[i]);

                            Meta.Write2DB();
                        }
                        else
                        {
                            string FileName = Path.GetFileName(Files[i]);
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

                            RealtimeData Realtime = new RealtimeData(Files[i]);

                            Realtime.Write2DB();
                        }
                    }
                    catch (Exception ex)
                    {
                        AlterMailService_Singleton.GetInisitance().Alter.SendMessage("FileMonitor_Created", ex.Message);
                    }
                }

                Temp_StartDate = Temp_StartDate.AddDays(1);
            }
        }

        private string[] GetFileList(string folderPath)
        {
            IComparer<string> Compa = new Comparison_DateModified();

            string[] FileName = null;

            try
            {
                FileName = Directory.GetFiles(folderPath, "*.xml");
            }
            catch (IOException ex)
            {
                Debug.WriteLine(ex.Message);
            }

            if (FileName == null)
            {
                return FileName;
            }

            List<string> FileNameList = new List<string>(FileName.Length);
            FileNameList.AddRange(FileName);
            FileNameList.Sort(Compa);

            return FileNameList.ToArray();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            _Config = new ManageConfig(Application.StartupPath + "\\Data\\config.xml");

            _Config.Read();

            _Timer = new Timer();
            _Timer.Interval = 1000 * 60 * 60 * 24;
            _Timer.Tick += new EventHandler(_Timer_Tick);
        }

        void _Timer_Tick(object sender, EventArgs e)
        {
            DateTime Yesterday = DateTime.Now.AddDays(-1);

            Import(Yesterday, Yesterday);
        }

        private void rabTimeSpan_CheckedChanged(object sender, EventArgs e)
        {
            if (this.rabTimeSpan.Checked)
            {
                this.dtpStart.Enabled = true;
                this.dtpEnd.Enabled = true;
            }
            else
            {
                this.dtpStart.Enabled = false;
                this.dtpEnd.Enabled = false;
            }
        }

        private void btnImport_Click(object sender, EventArgs e)
        {
            this.btnImport.Enabled = false;

            if (this.rabEveryday.Checked)
            {
                _Timer.Start();
            }
            else
            {
                DateTime Start = this.dtpStart.Value;
                DateTime End = this.dtpEnd.Value;

                Import(Start, End);

                this.btnImport.Enabled = true;
            }
        }
    }

    public class Comparison_DateModified : IComparer<string>
    {
        #region IComparer<string> Members

        public int Compare(string current, string previous)
        {
            return current.CompareTo(previous);
        }

        #endregion
    }
}
