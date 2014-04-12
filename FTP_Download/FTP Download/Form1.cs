using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using CommonFiles;
using System.Diagnostics;

namespace FTP_Download
{
    public partial class Form1 : Form
    {

        private ManageConfig _Config;

        private Timer _Timer_Realtime;
        private Timer _Timer_Meta;

        private ChangeFileName _ChangeFile_Realtime;
        private ChangeFileName _ChangeFile_Meta;

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            _Config = new ManageConfig(Application.StartupPath + "\\Data\\config.xml");
            _Config.Read();

            this.Text = _Config.Config.Local.FormName;

            _Timer_Realtime = new Timer();
            _Timer_Realtime.Interval = int.Parse(_Config.Config.Local.UpdateFrequency_Realtime);
            _Timer_Realtime.Tick += new EventHandler(_Timer_Realtime_Tick);

            _ChangeFile_Realtime = new ChangeFileName(_Config, Application.StartupPath + "\\Data\\modotrealtime_temp.tmp", _Config.Config.FTP.FilePath_RealtimeData, false);
            _Timer_Realtime.Start();


            //_Timer_Meta = new Timer();
            //_Timer_Meta.Interval = int.Parse(_Config.Config.Local.UpdateFrequency_Meta);
            //_Timer_Meta.Tick += new EventHandler(_Timer_Meta_Tick);

            //_ChangeFile_Meta = new ChangeFileName(_Config, Application.StartupPath + "\\Data\\modotmeta_temp.tmp", _Config.Config.FTP.FilePath_MetaData, true);
            //_Timer_Meta.Start();
        }

        void _Timer_Realtime_Tick(object sender, EventArgs e)
        {
           _ChangeFile_Realtime.CopyFile();
        }

        void _Timer_Meta_Tick(object sender, EventArgs e)
        {
            _ChangeFile_Meta.CopyFile();
        }
    }
}
