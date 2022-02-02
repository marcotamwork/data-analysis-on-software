# Program-of-my-summer-intern

Here's the file of task that I've finished in last summer(2020). All of it is about data analysis for software in HACTL Limited. An d my job is to find the user behaviour of software that userd by employees and customers in HACTL.

All of the materials I analysed is log file in company. Starting with daily and weekly database, I have analysed the whole year function at last (each month one by one). So the size of database will be at least around 800,000 logging record.

e.g. 
	[2020-01-01 00:00:00 xxx(system) AUTHORIZE ID:0000 authenticationService.authenticate() end]
...

(It's just 1 simple example, the actual log record will be far more detailed for all information.)

And the method I approached is to do data mining/crawling.

And lastly, due to Non-Disclosure Agreement, the code of data analysis program and data visualization program cannot be shown. For further details, the analysis is done with t-distribution and data visualization program is built with Bokeh.



## similarity for CPlus, CSS, BSS

CPlus, CSS, BSS are three main function/programme that used by employees(porters) in HACTL. This task is to find out similarity between these 3 programmes from aspect of **floors**, time, role. SD and mean are calculated as a stantard to measure the difference between **floors**. 

Example:
|Floor           |Role              |SD                           | Mean    |     
|----------------|-------------------|-----------------------------|-----------------------------|
|1|OTHERS|0.5xxxx min |2 min|
|2|OTHERS|0.4xxxx min|3 min|
|3|OTHERS|0.5xxxx min|4 min|
|4|OTHERS|0.3xxxx min|4 min|
|1|OC|0.4xxxx min|4 min|
|2|OC|0.6xxxx min|4 min|

...
...
> **Note:** Each Floor in HACTL has its speceific function. For example, floor 4 is for storing refrigerated cargo, floor 1 is to load cargo onto truck, etc. 

> **Note:** Role will be divide in to 'OTHERS', 'OC', 'SUPR', and 'MANAGER'.


## task3&4_git (analysis on CSS)

CSS is a programme which included numbers of function, eg. 'AdminConsole', 'CSS-RPT', 'CSS-SUP', etc (total 34 functions). The task is to count the time and function which employee has used in which floor and machine (Each floor has numbers of machine to run CSS). So here will have 4 dimensions: Time, Function, Person, Floor.

Example:
|Time |ID   |role |ip   |'AdminConsole'|'CSS-RPT'|'CSS-SUP'|...|
|-----|-----|-----|-----|--------------|---------|---------|---|
|06:13|01022|OTHERS|157.666.333.22|0|2|0|...|

...

> **Note:** This programme is to analyse the whole process when each employee used CSS. And it helps to store all logging record into database with their process record.

> **Note:** Time refers to time they spent on the machine(MIN:SEC). IP is to define which machine they are using. And the number under the function is the times that they have used the function in whole process.


## task5_git (analysis on CSS, BSS, CPlus)

This task is to analyse the data we storded with task3&4. Actually, I've done 3 version of this data which is specified for CSS, BSS and CPlus, which means that 3 database for CSS, BSS and CPlus was stored. **The whole task is to track the whole progress for each employee/customer truck. And it track the porter/truck with floor, time, functions, etc. **


Example:
|Start|End  |Time  |ID   |role |CPlus functions|CSS functions|BSS functions|
|-----|-----|------|-----|-----|---------------|-------------|-------------|
|12:00:00|12:06:13|06:13|01022|OTHERS|0|2, 'CSS-RPT', 'CSS-SUP'|0|
|12:06:13|12:08:13|02:00|01022|OTHERS|0|0|1, 'BSS-RPT'|

...

> **Note:** This programme is to analyse the whole process when employee/porter/truck do their daily work routine in company. And it helps to see how long they spent on each task.

