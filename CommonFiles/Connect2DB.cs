using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Windows.Forms;

namespace CommonFiles
{
    public class Connect2DB
    {
        static private SqlConnection _SqlConn;

        private Connect2DB()
        {
        }

        static public SqlConnection GetInisitance()
        {
            if (_SqlConn == null)
            {
                ManageConfig Config = new ManageConfig(Application.StartupPath + "\\Data\\config.xml");

                Config.Read();

                SqlConnectionStringBuilder ConnBuilder = new SqlConnectionStringBuilder();
                ConnBuilder.DataSource = Config.Config.Database.Name;
                ConnBuilder.InitialCatalog = Config.Config.Database.InitialCatalog;
                ConnBuilder.UserID = Config.Config.Database.User;
                ConnBuilder.Password = "Sql.slu2012";

                _SqlConn = new SqlConnection(ConnBuilder.ConnectionString);

            }

            if (_SqlConn.State != ConnectionState.Open)
            {
                _SqlConn.Open();
            }

            return _SqlConn;
        }

        
    }
}
