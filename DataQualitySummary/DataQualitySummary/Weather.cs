using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Diagnostics;
using System.IO;

namespace DataQualitySummary
{
    class Weather
    {
        /// <summary>
        /// The function that returns the current conditions for the specified location.
        /// </summary>
        /// <param name="location">City or ZIP code</param>
        /// <returns></returns>
        public static Conditions GetCurrentConditions()
        {
            Conditions conditions = new Conditions();

            XmlDocument xmlConditions = new XmlDocument();

            try
            {
                xmlConditions.Load(@"http://api.wunderground.com/api/e5aefd425d615a50/conditions/q/MO/Saint_Louis.xml");


                conditions.City = xmlConditions.SelectSingleNode("/response/current_observation/display_location/full").InnerText;
                conditions.Humidity = xmlConditions.SelectSingleNode("/response/current_observation/relative_humidity").InnerText;
                conditions.ObservationTime = xmlConditions.SelectSingleNode("/response/current_observation/observation_time_rfc822").InnerText;
                conditions.TempC = xmlConditions.SelectSingleNode("/response/current_observation/temp_c").InnerText;
                conditions.TempF = xmlConditions.SelectSingleNode("/response/current_observation/temp_f").InnerText;
                conditions.Wind_Degress = xmlConditions.SelectSingleNode("/response/current_observation/wind_degrees").InnerText;
                conditions.Wind_Dir = xmlConditions.SelectSingleNode("/response/current_observation/wind_dir").InnerText;
                conditions.Wind_String = xmlConditions.SelectSingleNode("/response/current_observation/wind_string").InnerText;
                conditions.Weather = xmlConditions.SelectSingleNode("/response/current_observation/weather").InnerText;

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }

            return conditions;
        }
    }

    class Conditions
    {
        private string _City = "Saint Louis";
        private string _ObservationTime;
        private string _TempF;
        private string _TempC;
        private string _Weather;

        
        private string _Humidity;
        private string _Wind_String;
        private string _Wind_Dir;
        private string _Wind_Degress;

        public void Output(StreamWriter sw)
        {
            //Debug.WriteLine("City: " + _City);
            //Debug.WriteLine("_ObservationTime: " + _ObservationTime);
            //Debug.WriteLine("_TempF: " + _TempF);
            //Debug.WriteLine("_TempC: " + _TempC);
            //Debug.WriteLine("_Humidity: " + _Humidity);
            //Debug.WriteLine("_Wind_String: " + _Wind_String);
            //Debug.WriteLine("_Wind_Dir: " + _Wind_Dir);
            //Debug.WriteLine("_Wind_Degress: " + _Wind_Degress);

            StringBuilder Sb = new StringBuilder();

            Sb.Append(_City + "\t");
            Sb.Append(_ObservationTime + "\t");
            Sb.Append(_Weather + "\t");
            Sb.Append(_TempF + "\t");
            Sb.Append(_TempC + "\t");
            Sb.Append(_Humidity + "\t");
            Sb.Append(_Wind_String + "\t");
            Sb.Append(_Wind_Dir + "\t");
            Sb.Append(_Wind_Degress);

            sw.WriteLine(Sb.ToString());

        }

        public string Weather
        {
            get { return _Weather; }
            set { _Weather = value; }
        }

        public string Wind_Degress
        {
            get { return _Wind_Degress; }
            set { _Wind_Degress = value; }
        }
        public string Wind_Dir
        {
            get { return _Wind_Dir; }
            set { _Wind_Dir = value; }
        }

        public string Wind_String
        {
            get { return _Wind_String; }
            set { _Wind_String = value; }
        }

        public string City
        {
            get { return _City; }
            set { _City = value; }
        }

        public string TempF
        {
            get { return _TempF; }
            set { _TempF = value; }
        }

        public string TempC
        {
            get { return _TempC; }
            set { _TempC = value; }
        }

        public string Humidity
        {
            get { return _Humidity; }
            set { _Humidity = value; }
        }

        public string ObservationTime
        {
            get { return _ObservationTime; }
            set { _ObservationTime = value; }
        }
    }

            

}
