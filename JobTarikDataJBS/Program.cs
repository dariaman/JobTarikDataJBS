using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Net.Mail;

namespace JobTarikDataJBS
{
    class Program
    {
        static string conString = ConfigurationManager.AppSettings["DefaultDB"].ToString();
        static string EmailAdmin = ConfigurationManager.AppSettings["AdminEmail"].ToString();

        static void Main(string[] args)
        {

            var NextJob = new List<ScheduleJobModel>();
            var JobExec = new List<ScheduleJobLogModel>();
            try
            {
                NextJob = poolJob();
            }
            catch (Exception ex)
            {
                SendEmail(EmailAdmin, "Error Job JBS (Pool Job) ", ex.Message);
                return;
            }

            try
            {
                PrepareInsertJobExec(NextJob);
            }
            catch (Exception ex)
            {
                SendEmail(EmailAdmin, "Error Job JBS (Insert Ke Job Log) ", ex.Message);
                return;
            }

            try
            {
                JobExec = poolJobExec();
            }
            catch (Exception ex)
            {
                SendEmail(EmailAdmin, "Error Job JBS (Pool Job EXEC) ", ex.Message);
                return;
            }

            try
            {
                JobExecution(JobExec);
            }
            catch (Exception ex)
            {
                SendEmail(EmailAdmin, "Error Job JBS (Insert Ke Job Log) ", ex.Message);
                return;
            }

            Console.WriteLine("Finish ... ");
            System.Threading.Thread.Sleep(5000);
        }

        private static List<ScheduleJobModel> poolJob()
        {
            var job = new List<ScheduleJobModel>();
            MySqlConnection con = new MySqlConnection(conString);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `ScheduleJob` sc WHERE sc.`IsAktif`=1 ORDER BY sc.`id`;", con);
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        job.Add(new ScheduleJobModel()
                        {
                            id = Convert.ToInt32(rd["id"]),
                            IsDaily = Convert.ToBoolean(rd["IsDaily"]),
                            IsMonthly = Convert.ToBoolean(rd["IsMonthly"]),
                            StepHour = (rd["StepHour"] == DBNull.Value) ? (int?)null : Convert.ToInt32(rd["StepHour"]),
                            tgl = (rd["tgl"] == DBNull.Value) ? (int?)null : Convert.ToInt32(rd["tgl"]),
                            waktuExecute = (rd["waktuExecute"] == DBNull.Value) ? (int?)null : Convert.ToInt32(rd["waktuExecute"]),
                            spName = rd["spName"].ToString(),
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("poolJob() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return job;
        }

        private static List<ScheduleJobLogModel> poolJobExec()
        {
            var job = new List<ScheduleJobLogModel>();
            MySqlConnection con = new MySqlConnection(conString);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"SELECT * FROM `ScheduleJobLog` sl WHERE sl.`IsExecute`=0 ORDER BY sl.`id`;", con);
            cmd.CommandType = CommandType.Text;
            try
            {
                con.Open();
                using (MySqlDataReader rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        job.Add(new ScheduleJobLogModel()
                        {
                            id = Convert.ToInt32(rd["id"]),
                            ScheduleJobID = Convert.ToInt32(rd["ScheduleJobID"]),
                            spName = rd["spName"].ToString(),
                            ScheduleTgl = Convert.ToDateTime(rd["ScheduleTgl"]),
                            ScheduleJam = Convert.ToInt32(rd["ScheduleJam"]),

                        });
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("poolJobExec() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }

            return job;
        }

        private static void PrepareInsertJobExec(List<ScheduleJobModel> listJob)
        {
            foreach (ScheduleJobModel job in listJob)
            {
                if (job.IsDaily)
                {
                    if (job.StepHour == null) // eksekusi per jam tertentu
                    {
                        if (job.waktuExecute == null) throw new Exception(" tanggal StepHour dan waktuExec sama-sama kosong");
                        if (DateTime.Now.Hour < job.waktuExecute) continue; //belum jam eksekusi
                    }
                    else // eksekusi dengan interval tertentu
                    {
                        if (DateTime.Now.Hour == 0)
                        {
                            InsertJobQueue(job); // jam 00 awal dari eksekusi job yg interval (gak bisa lolos dari validasi)
                            continue;
                        }
                        if (DateTime.Now.Hour % job.StepHour != 0) continue; // belum jam eksekusi
                    }

                    if ((job.StepHour == null) && (DateTime.Now.Hour != job.waktuExecute)) continue; //belum jam eksekusi
                }
                else if (job.IsMonthly) // cek job monthly
                {
                    if (job.tgl != DateTime.Now.Day) continue; // jika belum tiba tanggal saatnya, proses skip
                    //jika tanggal sama, maka cek jam eksekusi
                    if ((job.waktuExecute < 0) || (job.waktuExecute == null)) throw new Exception("Job Monthly, jam eksekusi kosong"); // proses jadi break
                    if (job.waktuExecute > DateTime.Now.Hour) continue; // jika belum jamnya, proses skip
                }
                InsertJobQueue(job);
            }
        }

        private static void JobExecution(List<ScheduleJobLogModel> listJob)
        {
            MySqlConnection con = new MySqlConnection(conString);
            MySqlConnection conP = new MySqlConnection(conString);
            MySqlCommand cmd;
            MySqlCommand cmdP;

            foreach (ScheduleJobLogModel job in listJob)
            {
                Console.WriteLine("Proses " + job.spName + " . . . ");
                try
                {
                    /// Update tuk buat start date
                    cmd = new MySqlCommand(@"UPDATE `ScheduleJobLog` sl SET sl.`StartDate`=NOW() WHERE sl.`id`=@id;", con);
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = job.id });
                    con.Open();
                    cmd.ExecuteNonQuery();
                    con.Close();

                    // exec SP Job
                    cmd = new MySqlCommand(job.spName, con);
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Clear();
                    con.Open();
                    cmd.ExecuteNonQuery();
                    con.Close();

                    /// Update tuk buat END date dan IsExec
                    cmd = new MySqlCommand(@"UPDATE `ScheduleJobLog` sl SET sl.`FinishDate`=NOW(),sl.`IsExecute`=1 WHERE sl.`id`=@id;", con);
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = job.id });
                    con.Open();
                    cmd.ExecuteNonQuery();
                    con.Close();
                }
                catch (Exception ex)
                {
                    if (con.State == ConnectionState.Open) con.Close();
                    /// Update tuk buat END date dan IsExec
                    cmdP = new MySqlCommand(@"UPDATE `ScheduleJobLog` sl SET sl.`Note`=@note WHERE sl.`id`=@id;", conP);
                    cmdP.CommandType = CommandType.Text;
                    cmdP.Parameters.Clear();
                    cmdP.Parameters.Add(new MySqlParameter("@id", MySqlDbType.Int32) { Value = job.id });
                    cmdP.Parameters.Add(new MySqlParameter("@note", MySqlDbType.VarChar) { Value = ex.Message });
                    conP.Open();
                    cmdP.ExecuteNonQuery();
                    conP.Close();

                    SendEmail(EmailAdmin, "Error Job JBS (JobExecution) ", ex.Message);
                    return;
                }
                finally
                {
                    if (con.State == ConnectionState.Open) con.Close();
                    if (conP.State == ConnectionState.Open) conP.Close();
                }
            }
        }

        private static void SendEmail(string EmailTo, string Subject, string Body)
        {
            SmtpClient mailClient = new SmtpClient();
            mailClient.Host = "mail.caf.co.id";
            mailClient.UseDefaultCredentials = true;
            mailClient.Port = 25;

            MailAddress from = new MailAddress("no-reply@jagadiri.co.id");
            MailAddress to = new MailAddress(EmailTo);

            MailMessage message = new MailMessage(from, to);
            message.IsBodyHtml = true;
            message.Subject = Subject;
            message.Body = Body;

            mailClient.Send(message);

        }

        private static void InsertJobQueue(ScheduleJobModel job)
        {
            MySqlConnection con = new MySqlConnection(conString);
            MySqlCommand cmd;
            cmd = new MySqlCommand(@"InsertScheduleJobLog", con);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Clear();
            cmd.Parameters.Add(new MySqlParameter("@SkedulJobID", MySqlDbType.Int32) { Value = job.id });
            cmd.Parameters.Add(new MySqlParameter("@sp", MySqlDbType.VarChar) { Value = job.spName });
            cmd.Parameters.Add(new MySqlParameter("@jam", MySqlDbType.Int32) { Value = job.waktuExecute ?? DateTime.Now.Hour });
            try
            {
                con.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception("InsertJobQueue() : " + ex.Message);
            }
            finally
            {
                con.Close();
            }
        }
    }
}
