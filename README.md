Basic XML Parser
================

currently this swallows xml files that come from TR's DB and outputs SQL that we can stick in our DB for analysis. 
 
Data Schema
===========
This block of code creates the schema needed for the output of the ParseXML jar: 
 
    delete from x;
    delete from subject_hash;
    delete from author_country_year;

    drop table x;
    drop table author_country_year;
    drop table subject_hash;
    
    CREATE TABLE x
    (
    year int,
    author varchar(255),
    subject_hash int,
    value_total decimal
    );

    CREATE TABLE author_country_year
    (
    author varchar(255),
    country varchar(255),
    year int
    );
        
    CREATE TABLE subject_hash
    (
    subject varchar(255) UNIQUE,
    hash BIGINT UNIQUE
    );
    
    CREATE INDEX ac_country_index ON  author_country_year (country);
    CREATE INDEX ac_year_index ON  author_country_year (year);
    CREATE INDEX x_year_index ON x (year);
    
    

the SecondPhase processor uses the following SQL schema:
 
    delete from rca_country_year;
    drop table rca_country_year;
    
    delete from year_country_subject_x;
    drop table year_country_subject_x;
    
    CREATE TABLE year_country_subject_x
    (
    year int,
    country varchar(255),
    subject_hash int,
    x decimal
    );

SecondPhase
===========
the SecondPhase reads in files named year.xml 


SubSubject
==========
the SubSubject processor is specifically used to find correlations between subSubjects by processing their UIDs. The 
input is a csv file given as an argument and the output is a MMA input steam sent to standard out.