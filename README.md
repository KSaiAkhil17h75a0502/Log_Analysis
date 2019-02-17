
<h1>Log analysis</h1>
<p>In this project, we create a internal tool by using python that interacts with a portgese sql database to retrieve some information.</p>
<h2>This project helps to draw conclusions for:</h2>
1.what are the most popular three articles of all time?<br>
2.who are the most popular article authors of all time?<br>
3.On which day more than 1% of requests leads to errors?<br>
<h2>Before making this project we need to have knowledge about some tools and languages:</h2>
1.<a href="https://www.python.org/
">python</a><br>
2.<a href="https://www.vagrantup.com/">vagrant</a><br>
3.<a href="https://www.virtualbox.org/">virtual box</a><br>
<h2>Set up the vagrant:</h2>
1.Install Vagrant and VirtualBox
<p>2.Download or clone the <a href="https://s3.amazonaws.com/video.udacity-data.com/topher/2018/April/5acfbfa3_fsnd-virtual-machine/fsnd-virtual-machine.zipfullstack-nanodegree-vm"> FSND-Virtual_Machine</a> repository. Make sure that you downloaded this folder because it has some inbuilt packages.If we install ubuntu manually by using “vagrant init trusty/ubuntu64” which doesn’t  contains all the packages that we goanna need in this project and in order to use them we need to install each and every project manually instead of that all the package are available in ubuntu version given in that file.</p>
3.Downlaod the <a href="https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zipdata">data</a> from here.
<br>4.Make a new directory and unzip the folder “FSND-Virtual-Machine” into it.
<p>5.Now open the vagrant folder in FSND-Virtual-Machine and unzip the data file “newdata”  to vagrant folder.</p>
<h2>Installing the virtual machine:</h2>
1.Install the vagrant and virtual Box.
<p>.After installing both of them open the terminal and change to the directory where vagrant folder is present</p>
3.Now use the vagrant up to create and download the virtual machine.<br>
4.Virtual machine got installed and use vagrant ssh to use the virtual machine.
<h2>Accessing the virtual machine:</h2>
1.Make sure you have changed to the path where you installed vagrant<br>
2.First use, this command to start the virtual box
<br>$vagrant up <br>
3.use this command to access the ubuntu terminal
<br>$vagrant ssh<br>
4. Afterwards you are provided with ubuntu.<br>
<p>5. vagrant has given us the option to share a particular location to access the files both in host system and virtual machine i.e the place where ‘.vagrant’ file found it is the shared location in which we can create or copy files and access them  both on host and virtual systems.(generally .vagrant file is present in vagrant folder in our project).</p>
6.To access that directory “cd /vagrant”. It will change to shared directory<br>
7.To view all the files use
<br>’ls'<br>
8.To create a database 
$psql #super user vagrant.<br>
>create database database-name<br>
Database created.<br><p> To access this use ‘\c database-name’.or you can use this command ‘psql databsename’</p>
9. Push the ‘newsdata.sql’ into database news
  <br> $psql -d news -f newsdata.sql<br>
   Data got dumped to the news database

  <h2>Reporting tool:</h2>
  <b> $python tool.py</b>

<h2>Some pages that I refer for this project:</h2>
1.<a href="https://stackoverflow.com/questions/30423849/psql-fatal-role-vagrant-does-not-exist">problem with vagrant</a><br> 2.<a href="https://stackoverflow.com/questions/7057450/why-does-python-use-unconventional-triple-quotation-marks-for-comments">python quotation</a><br> 3.<a href=" https://stackoverflow.com/questions/18934686/convert-datetime-object-of-type-b-d-y">formatting output problem in python</a><br> 4. https://stackoverflow.com/questions/38538406/why-do-we-use-format-identifier-python <br>5. https://www.postgresql.org/docs/9.2/app-psql.html<br> 6. https://wiki.python.org/moin/BeginnersGuide <br>7. https://www.vagrantup.com/docs/index.html  
