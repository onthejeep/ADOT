using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Mail;
using System.Diagnostics;

namespace CommonFiles
{
    public class AlterMailService
    {
        private MailMessage _MailMess;
        private SmtpClient _SmtpServer;

        public AlterMailService()
        {

            _MailMess = new MailMessage();
            _SmtpServer = new SmtpClient("smtp.gmail.com");

            _MailMess.From = new MailAddress("smarttranslab@gmail.com");
            _MailMess.To.Add("shuyang@email.arizona.edu");
            
            _MailMess.IsBodyHtml = true;


            _SmtpServer.Port = 587;
            _SmtpServer.Credentials = new System.Net.NetworkCredential("smarttranslab", "googleaccountpwd");
            _SmtpServer.EnableSsl = true;

        }

        public void SendMessage(string subject, string message)
        {
            try
            {
                _MailMess.Body = message;

                _MailMess.Subject = subject;

                _SmtpServer.Send(_MailMess);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.ToString());
            }
        }

        public void SendMessage(Exception ex)
        {
            string SendMessage = string.Format(
                    "<b>TargetSite:</b>{0}<br><b>StackTrace:</b>{1}<br><b>Message:</b>{2}",
                    ex.TargetSite,
                    ex.StackTrace,
                    ex.Message);

            string Subject = ex.Message.Substring(0, 50);

            try
            {
                _MailMess.Body = SendMessage;

                _MailMess.Subject = Subject;

                _SmtpServer.Send(_MailMess);
            }
            catch (Exception e)
            {
                Debug.WriteLine("AlterMailService.SendMessage(Exception ex)" + e.ToString());
            }
        }
    }

    public class AlterMailService_Singleton
    {
        private AlterMailService _Alter;

        public AlterMailService Alter
        {
            get { return _Alter; }
        }

        private static AlterMailService_Singleton _Singleton = null;

        private AlterMailService_Singleton()
        {
            _Alter = new AlterMailService();
        }

        public static AlterMailService_Singleton GetInisitance()
        {
            if (_Singleton == null)
            {
                _Singleton = new AlterMailService_Singleton();
            }

            return _Singleton;
        }
    }
}
