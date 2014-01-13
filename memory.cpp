/*
 * memory.cpp
 *
 *  Created on: 2013年12月25日
 *      Author: wangyang 
 */
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <db_cxx.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <time.h>
#include <signal.h>
#include <set>
#define  MAX_LAC 500
using namespace std;

typedef  struct {
    Db      *imsi_p;
    Db      *work_p;
    Db		*home_p;
    Db		*time_p;
   }  db_stat;
typedef  struct
{
       int lac;
       int cell;
       int at_cnt;
}  used_lac;

typedef  struct
{
       int lac;
       int cell;
}  used_last;
/*定义全局路径*/

/*每天提取IMSI和手机号码的对应关系*/
const int c_month_tm[12] = {
	  0,
	  86400*(31),
	  86400*(31+29),
	  86400*(31+29+31),
	  86400*(31+29+31+30),
	  86400*(31+29+31+30+31),
	  86400*(31+29+31+30+31+30),
	  86400*(31+29+31+30+31+30+31),
	  86400*(31+29+31+30+31+30+31+31),
	  86400*(31+29+31+30+31+30+31+31+30),
	  86400*(31+29+31+30+31+30+31+31+30+31),
	  86400*(31+29+31+30+31+30+31+31+30+31+30)
	 };

int stop_flag=0;
const char *app_log="/archlog/bdb/applog";
const char *src_path="/archlog/work_home/ler";
const char *imei_path="/archlog/work_home/imei";
const char *day_path="/archlog/bdb/home_work/out";

int openDb(Db **, const char *, const char *, DbEnv *, u_int32_t);
void *deal_ler(void *);
void *update_imsi (void *);
void *out_user(void *);

time_t make_times(struct tm *m)
{
	 time_t llseconds = 0;
	 int year = m->tm_year;
	 int years=year-70;
	 int yunDays=0;

	 yunDays =((years+2)/4 - (((year+1900)/100 - 1970/100)/4*3+((year+1900)/100 - 1970/100)%4-1)) ;
	 llseconds = 365*86400*(time_t)years + 86400*(time_t)yunDays;

	 llseconds += c_month_tm[m->tm_mon];
	 llseconds += 86400*(m->tm_mday-1);
	 llseconds += 3600*m->tm_hour;
	 llseconds += 60*m->tm_min;
	 llseconds += m->tm_sec;

	 if (m->tm_mon>1 && !(((year%4==0) && (year%100!=0))||(year%400==0)))
		 llseconds -= 86400;

	 if((((year%4==0) && (year%100!=0))||(year%400==0)))
	     llseconds -= 86400;

	 llseconds -= 8*3600;
	 return (llseconds);
}

void sig_stop( int signo )
{
  signal(SIGUSR1,sig_stop);
  stop_flag=1;
}

int get_time_hour(time_t t)
{
    struct tm tmp_tm;
    localtime_r(&t,&tmp_tm);
     return (tmp_tm.tm_hour);
}

void trim( char *str)
{
     for (int j=strlen(str)-1;j>=0;j--)
     {
       if( isspace(str[j]) ) str[j]='\0';
       else break;
     }
}

size_t getNextPound(std::string &theString, std::string &substring,std::string token)
{
    size_t pos ;
    if(theString.size()<=0)
    {
      substring.clear();
      return(0);
    }
    pos= theString.find(token);
    if(pos==string::npos && theString.size()>0)
    {
     substring=theString;
     theString.clear();
    }
    else
    {
     substring.assign(theString, 0, pos);
     theString.assign(theString, pos + 1, theString.size());
    }
    return (pos);
}
unsigned long get_long_nu(char *src)
{
    char a[100];
    char *b;
    strcpy(a,src);
    b=a+strlen(src);
    return strtoul(a, &b, 10);
}

time_t get_time_t(const char *str)
{
    struct tm tmp_tm;
    char tmp[30];
    int  y = 0;
    int  m = 0;
    int  d = 0;
    int  h = 0;
    int  min = 0;
    int  sec = 0;
    strcpy(tmp,str);
    trim(tmp);
    sscanf(tmp,"%04d%02d%02d%02d%02d%02d",&y,&m,&d,&h,&min,&sec);
     tmp_tm.tm_year=y - 1900;
     tmp_tm.tm_mon=m - 1;
     tmp_tm.tm_mday=d;
     tmp_tm.tm_hour=h;
     tmp_tm.tm_min=min;
     tmp_tm.tm_sec=sec;
     return (make_times(&tmp_tm));
}

int main(void) {
	DbEnv *envp = NULL;
	pthread_t p_thread[5];
	db_stat db_info;
	u_int32_t envFlags;

	envFlags =
	DB_CREATE |
	DB_INIT_LOCK |
	DB_INIT_LOG |
	DB_SYSTEM_MEM |
	DB_INIT_MPOOL |
	DB_THREAD;

	envp = new DbEnv(0);
	envp->set_cachesize(30, 4096, 0);
	envp->set_tx_max(2000);
	envp->set_shm_key(0X7000);
	envp->set_lk_max_locks(2000);
	envp->set_lk_detect(DB_LOCK_MAXLOCKS);

	//envp->set_lg_bsize(u_int32_t(1024 * 1024 * 1024 * 500));

	envp->set_error_stream(&std::cerr);
	envp->set_message_stream(&std::cout);
	envp->set_errpfx("work_home_exe");

	envp->set_data_dir("/db2home/home_work");

	envp->open("/db2home/home_work", envFlags, 0);

	openDb(&db_info.imsi_p, "ims.db", NULL, envp, 0);
	openDb(&db_info.work_p, "work.db", NULL, envp, 0);
	openDb(&db_info.home_p, "home.db", NULL, envp, 0);
	openDb(&db_info.time_p, "time.db", NULL, envp, 0);

	signal(SIGUSR1, sig_stop);

	pthread_create(&p_thread[0], NULL, update_imsi, (void *) db_info.imsi_p);
	pthread_create(&p_thread[1], NULL, deal_ler, (void *) &db_info);
	pthread_create(&p_thread[2], NULL, out_user, (void *) &db_info);

	for (int i = 0; i <= 2; i++) {
		(void) pthread_join(p_thread[i], NULL);
	}
	envp->close(0);
	std::cout << "all done quite successed" << std::endl;
	return (0);
}


void *update_imsi(void *args)
{
	Db *imsi_dbp;
	DIR *dirp;
	std::string stringBuf;
	std::string substring;
	struct dirent *direntp;
	struct stat f_info;
	char file[156];
	char imsi[16];
	char bill_id[12];
	unsigned long l_imsi;
	Dbt key, value;
	imsi_dbp = (Db *) args;
	//envp = imsi_dbp->get_env();

	std::ifstream inFile;
	while (stop_flag <= 0) {
		sleep(60);
		if ( NULL == (dirp = opendir(imei_path)))
		{
			cout << "打开路径:" << imei_path << "error" << endl;
			continue;
		}
		cout << "开始扫描路径:" << imei_path << endl;
		while ((direntp = readdir(dirp)) != NULL)
		{
			if (strcmp(direntp->d_name, ".") == 0
					|| strcmp(direntp->d_name, "..") == 0)
				continue;
			strcpy(file, imei_path);
			strcat(file, "/");
			strcat(file, direntp->d_name);
			cout << "开始处理文件:" << file << endl;
			if ((stat(file, &f_info) != 0) || ( S_ISREG(f_info.st_mode) != 1))
				continue;
			inFile.open(file, std::ios::in);
			if (!inFile)
			{
				std::cerr << "Could not open imsi file " << std::endl;
				continue;
			}
			while (!inFile.eof())
			{
				std::getline(inFile, stringBuf);

				if (stringBuf.empty())
					continue;
				getNextPound(stringBuf, substring, ",");
				strcpy(imsi, substring.c_str());
				trim(imsi);
				if (strlen(imsi) != 15)
					continue;
				getNextPound(stringBuf, substring, ",");
				strcpy(bill_id, substring.c_str());
				trim(bill_id);
				if (strlen(bill_id) != 11)
					continue;
				l_imsi = get_long_nu(imsi);
				key.set_data(&l_imsi);
				key.set_size(sizeof(unsigned long));
				value.set_data(bill_id);
				value.set_flags(DB_DBT_USERMEM);
				value.set_size(sizeof(bill_id));
				imsi_dbp->put(NULL, &key, &value, 0);
			}
			inFile.close();
			remove(file);
		}
		closedir(dirp);
	}
	return (0);
}

int openDb(Db **dbpp, const char *fileName, const char *db_name, DbEnv *envp, u_int32_t extraFlags)
{
	u_int32_t openFlags;

	Db *dbp = new Db(envp, 0);
	*dbpp = dbp;
	if (extraFlags != 0)
		dbp->set_flags(extraFlags);
	openFlags = DB_CREATE | DB_THREAD;

	dbp->open(NULL, fileName, db_name, DB_BTREE, openFlags, 0);
	return (0);
}

void *deal_ler (void *args)
{
	db_stat *info_db;
	Db *imsi_dbp, *home_dbp, *work_dbp, *time_dbp;
	std::string stringBuf;
	std::string stringbuf1;
	std::string substring;
	DIR *dirp;
	char file[156];
	struct dirent *direntp;
	/*话单信息*/
	char imsi[20];
	char bill_id[20];
	char timestamp[20];
	char event[10];

	time_t event_time;
	struct stat f_info;
	used_lac user_loc_info;
	used_lac *user_loc_buff;
	used_last user_last_info;
	unsigned long l_imsi;
	int m, n;
	int ret;
	//clock_t   b_time ,e_time;
	Dbt key, value;
	info_db = (db_stat *) args;
	imsi_dbp = info_db->imsi_p;
	home_dbp = info_db->home_p;
	work_dbp = info_db->work_p;
	time_dbp = info_db->time_p;
	//envp = imsi_dbp->get_env();

	set<string> filename;
	set<string>::iterator filename_p;

	std::ifstream inFile;

	// std::ofstream err_f;
	user_loc_buff = NULL;
	user_loc_buff = (used_lac *) malloc(sizeof(used_lac) * MAX_LAC);
	if (user_loc_buff == NULL) {
		cout << "分配内存失败" << endl;
		return 0;
	}
	while (stop_flag <= 0)
	{
		sleep(20);
		if ( NULL == (dirp = opendir(src_path)))
		{
			cout << "打开路径:" << src_path << "error" << endl;
			continue;
		}
		filename.clear();
		while ((direntp = readdir(dirp)) != NULL)
		{
			if (strcmp(direntp->d_name, ".") == 0
					|| strcmp(direntp->d_name, "..") == 0)
				continue;
			strcpy(file, src_path);
			strcat(file, "/");
			strcat(file, direntp->d_name);
			if ((stat(file, &f_info) != 0) || ( S_ISREG(f_info.st_mode) != 1))
				continue;
			filename.insert(file);
		}
		closedir(dirp);
		cout << "开始扫描路径:" << src_path << endl;
		for (filename_p = filename.begin(); filename_p != filename.end();
				++filename_p)
		{
			inFile.clear();
			inFile.open(filename_p->c_str(), std::ios::in);
			cout << "开始处理文件 ----" << filename_p->c_str() << endl;
			if (!inFile)
			{
				std::cerr << "Could not open ler file " << std::endl;
				cout << "Could not open ler file :" << file << endl;
				continue;
			}
			while (!inFile.eof())
			{
				std::getline(inFile, stringBuf);
				stringbuf1 = stringBuf;
				if (stringBuf.empty())
					continue;
				getNextPound(stringBuf, substring, ",");
				strcpy(imsi, substring.c_str());
				trim(imsi);
				if (strlen(imsi) != 15 || imsi[0] == '#'
						|| (strncmp(imsi, "460", 3) != 0))
				{
					continue;
				}
				getNextPound(stringBuf, substring, ",");
				if (strncmp(substring.c_str(), "86", 2) == 0)
					strcpy(bill_id, substring.substr(2).c_str());
				else
					strcpy(bill_id, substring.c_str());
				trim(bill_id);
				if (strlen(bill_id) <= 11 && strlen(bill_id) >= 1)
				{
					continue;
				}
				memset(&user_loc_info,'\0',sizeof(used_lac));
				getNextPound(stringBuf, substring, ",");
				strcpy(timestamp, substring.substr(0, 14).c_str());
				event_time = get_time_t(timestamp);
				getNextPound(stringBuf, substring, ","); //lac
				user_loc_info.lac=atoi(substring.c_str());
				getNextPound(stringBuf, substring, ","); //ci
				user_loc_info.cell=atoi(substring.c_str());
				getNextPound(stringBuf, substring, ","); //event
				strcpy(event, substring.c_str());
				trim(event);
				if (strlen(event) <= 0)
				{
					continue;
				}
				if (strcmp(event, "05.08.1") != 0
						&& strcmp(event, "05.01.0") != 0)
					continue;
				if ((get_time_hour(event_time) >1  && get_time_hour(event_time) < 10 )
						|| (get_time_hour(event_time) > 11 && get_time_hour(event_time) < 14)
						|| (get_time_hour(event_time) > 15 && get_time_hour(event_time) < 20))
					continue;
				l_imsi = 0;
				l_imsi =get_long_nu(imsi);
				key.set_data(&l_imsi);
				key.set_size(sizeof(unsigned long));

				if (imsi_dbp->exists(NULL, &key, 0) == DB_NOTFOUND)
				{
					continue;
				}
              //工作时间
              if( get_time_hour(event_time) ==10 ||  get_time_hour(event_time) ==11
            	 || get_time_hour(event_time) ==14 || get_time_hour(event_time) ==15 )
              {
					//判断工作时间表中是否有数据
					key.set_data(&l_imsi);
					key.set_size(sizeof(unsigned long));
					value.set_data(user_loc_buff);
					value.set_flags(DB_DBT_USERMEM);
					value.set_ulen(sizeof(used_lac) * MAX_LAC);
					if ((ret = work_dbp->get(NULL, &key, &value, 0))
							== DB_NOTFOUND)
					{
						user_loc_info.at_cnt = 1;
						key.set_data(&l_imsi);
						key.set_size(sizeof(unsigned long));
						value.set_data(&user_loc_info);
						value.set_flags(DB_DBT_USERMEM);
						value.set_size(sizeof(used_lac));
						work_dbp->put(NULL, &key, &value, 0);

					} else
					{
						n = value.get_size() /sizeof(used_lac);
						for (m = 0; m < n; m++)
						{
							if (user_loc_buff[m].cell == user_loc_info.cell
									&& user_loc_buff[m].lac
											== user_loc_info.lac)
							{
								user_loc_buff[m].at_cnt++;
								value.set_size(sizeof(used_lac) * n);
								work_dbp->put(NULL, &key, &value, 0);
								break;
							}
						}
						if (m == n && n < (MAX_LAC - 1))
						{
							user_loc_info.at_cnt = 1;
							user_loc_buff[n] = user_loc_info;
							key.set_data(&l_imsi);
							key.set_size(sizeof(unsigned long));
							value.set_data(user_loc_buff);
							value.set_flags(DB_DBT_USERMEM);
							value.set_size(sizeof(used_lac) * (m + 1));
							work_dbp->put(NULL, &key, &value, 0);
						}
					}
              }
              else if ( get_time_hour(event_time) ==0 ||  get_time_hour(event_time) ==1)
              {
					key.set_data(&l_imsi);
					key.set_size(sizeof(unsigned long));
					value.set_data(user_loc_buff);
					value.set_flags(DB_DBT_USERMEM);
					value.set_ulen(sizeof(used_lac) * MAX_LAC);
					if ((ret = home_dbp->get(NULL, &key, &value, 0))
							== DB_NOTFOUND)
					{
						user_loc_info.at_cnt = 1;
						key.set_data(&l_imsi);
						key.set_size(sizeof(unsigned long));
						value.set_data(&user_loc_info);
						value.set_flags(DB_DBT_USERMEM);
						value.set_size(sizeof(used_lac));
						home_dbp->put(NULL, &key, &value, 0);

					} else
					{
						n = value.get_size() / sizeof(used_lac);
						for (m = 0; m < n; m++)
						{
							if (user_loc_buff[m].cell == user_loc_info.cell
									&& user_loc_buff[m].lac
											== user_loc_info.lac)
							{
								user_loc_buff[m].at_cnt++;
								value.set_size(sizeof(used_lac) * n);
								home_dbp->put(NULL, &key, &value, 0);
								break;
							}
						}
						if (m == n && n < (MAX_LAC - 1))
						{
							user_loc_info.at_cnt = 1;
							user_loc_buff[n] = user_loc_info;
							key.set_data(&l_imsi);
							key.set_size(sizeof(unsigned long));
							value.set_data(user_loc_buff);
							value.set_flags(DB_DBT_USERMEM);
							value.set_size(sizeof(used_lac) * (m + 1));
							home_dbp->put(NULL, &key, &value, 0);
						}
					}
            	}
            	else if ( (get_time_hour(event_time) >=20 && get_time_hour(event_time) <=23 )
            			|| (get_time_hour(event_time) >=0 && get_time_hour(event_time) <=3 )  )
            	{
            		key.set_data(&l_imsi);
            		key.set_size(sizeof(unsigned long));

            		if(home_dbp->exists(NULL,&key,0) == DB_NOTFOUND)
            		{
            			user_last_info.cell = user_loc_info.cell;
            			user_last_info.lac  = user_loc_info.lac;
						key.set_data(&l_imsi);
						key.set_size(sizeof(unsigned long));
						value.set_data(&user_last_info);
						value.set_flags(DB_DBT_USERMEM);
						value.set_size(sizeof(used_last));
						time_dbp->put(NULL, &key, &value, 0);
            		}
            	}
                continue;
		}
        inFile.close();
        remove(filename_p->c_str());
    }
  }
    return (0);
}

void *out_user(void *args) 
{
	Db *imsi_dbp, *home_dbp, *work_dbp, *time_dbp;
	Dbc *cursor_p;
	Dbt imsi_key, imsi_value;
	Dbt work_key, work_value;
	Dbt home_key, home_value;
	Dbt time_key, time_value;
	db_stat *info_db;
	unsigned long l_imsi, l_bill_id;
	used_lac *user_loc_buff;
	used_last user_last_info;
	struct tm tmp_tm;
	char out_file[156];
	int ret;
	info_db = (db_stat *) args;
	imsi_dbp = info_db->imsi_p;
	home_dbp = info_db->home_p;
	work_dbp = info_db->work_p;
	time_dbp = info_db->time_p;
	//envp = imsi_dbp->get_env();
	time_t now_time,old_time;
	short hour;
	int m,n;
	std::ofstream out_f;

	user_loc_buff = NULL;
	user_loc_buff = (used_lac *) malloc(sizeof(used_lac) * MAX_LAC);
	if (user_loc_buff == NULL) {
		cout << "分配内存失败" << endl;
		return 0;
	}
	old_time = 0;
	/* Check once every 5 minutes. */
	while (stop_flag <= 0)
	{
		sleep(60);
		now_time = time(NULL);
		hour = get_time_hour(now_time);
		localtime_r(&now_time,&tmp_tm);
		memset(out_file,'\0',sizeof(out_file));
		sprintf(out_file,"%s/%04d%02d%02d%02d%02d%02d.del",day_path,tmp_tm.tm_year+1900,tmp_tm.tm_mon+1,
				tmp_tm.tm_mday,tmp_tm.tm_hour,tmp_tm.tm_min,tmp_tm.tm_sec);
		out_f.clear();
		out_f.open(out_file);
		if (hour == 8 &&  now_time >= (old_time +86400-500))
		{
			//定义cursor
			old_time=now_time;
			imsi_key.set_data(&l_imsi);
			imsi_key.set_flags(DB_DBT_USERMEM);
			imsi_key.set_ulen(sizeof(unsigned long));
			imsi_key.set_size(0);
			imsi_value.set_data(&l_bill_id);
			imsi_value.set_flags(DB_DBT_USERMEM);
			imsi_value.set_ulen(sizeof(unsigned long));
			imsi_dbp->cursor(NULL, &cursor_p, 0);
			while ((ret = cursor_p->get(&imsi_key, &imsi_value, DB_NEXT))!= DB_NOTFOUND)
			{
				//通过imsi获得工作时间的用户
				work_key.set_data(&l_imsi);
				work_key.set_size(sizeof(unsigned long));
				work_value.set_data(user_loc_buff);
				work_value.set_flags(DB_DBT_USERMEM);
				work_value.set_ulen(sizeof(used_lac) * MAX_LAC);
				memset(user_loc_buff,'\0',sizeof(used_lac) * MAX_LAC);
				if ((ret = work_dbp->get(NULL, &work_key, &work_value, 0))
						!= DB_NOTFOUND)
				{
					n = work_value.get_size() / sizeof(used_lac);
					for (m = 0; m < n; n++) {
						out_f << l_bill_id << "," << 1 << ","
								<< user_loc_buff[m].lac << ","
								<< user_loc_buff[m].cell << ","
								<< user_loc_buff[m].at_cnt << endl;
					}
				}
				home_key.set_data(&l_imsi);
				home_key.set_size(sizeof(unsigned long));
				home_value.set_data(user_loc_buff);
				home_value.set_flags(DB_DBT_USERMEM);
				home_value.set_ulen(sizeof(used_lac) * MAX_LAC);
				memset(user_loc_buff,'\0',sizeof(used_lac) * MAX_LAC);
				if ((ret = work_dbp->get(NULL, &home_key, &home_value, 0))!= DB_NOTFOUND)
				{
					n=work_value.get_size()/sizeof(used_lac);
					for (m = 0; m < n; n++) {
						out_f << l_bill_id << "," << 2 << ","
								<< user_loc_buff[m].lac << ","
								<< user_loc_buff[m].cell << ","
								<< user_loc_buff[m].at_cnt << endl;
					}
				} else
				{
					time_key.set_data(&l_imsi);
					time_key.set_size(sizeof(unsigned long));
					time_value.set_data(&user_last_info);
					time_value.set_flags(DB_DBT_USERMEM);
					time_value.set_ulen(sizeof(user_last_info));
					if ((ret = time_dbp->get(NULL, &time_key, &time_value, 0)) == 0 )
					{
						out_f << l_bill_id << "," << 2 << ","
								<< user_last_info.lac << ","
								<< user_last_info.cell << ","
								<< 1 << endl;
					}
				}
			}
			out_f.close();
			out_f.clear();
			home_dbp->truncate(NULL,NULL,0);
			home_dbp->truncate(NULL,NULL,0);
			home_dbp->truncate(NULL,NULL,0);
		}

   }
  return (0);
}


