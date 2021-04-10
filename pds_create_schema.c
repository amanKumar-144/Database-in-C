#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "academia.h"
#include "pds.h"

struct PDS_DBInfo handle;

void process_line( char *test_case )
{	
	char dataType[30];
    char entityName[30];int entityRecordSize;
    char relationShipName[30];char entityOne[30];char entityTwo[30];
    sscanf(test_case,"%s ",dataType);
    if(strcmp(dataType,"entity")==0)
    {
        char redundant[30];
        sscanf(test_case,"%s %s %d",redundant,entityName,&entityRecordSize);
        //printf("%s %d\n",entityName,entityRecordSize);

        struct PDS_EntityInfo e;
        strcpy(e.entity_name,entityName);
        e.entity_size=entityRecordSize;
        handle.entities[handle.num_entities]=e;
        handle.num_entities++;
    }
    else if(strcmp(dataType,"relationship")==0)
    {
        char redundant[30];
        sscanf(test_case,"%s %s %s %s",redundant,relationShipName,entityOne,entityTwo);
        //printf("%s %s %s\n",relationShipName,entityOne,entityTwo);

        struct PDS_LinkInfo r;
        strcpy(r.link_name,relationShipName);
        strcpy(r.pds_name1,entityOne);
        strcpy(r.pds_name2,entityTwo);
        handle.links[handle.num_relationships]=r;
        handle.num_relationships++;
    }
    else
    {
        //printf("The data File name is %s\n",dataType);
        strcpy(handle.db_name,dataType);
        
    }
}
int main(int argc, char *argv[])
{
    handle.num_entities=0;handle.num_relationships=0;
	FILE *cfptr;
	char test_case[50];

	if( argc != 2 ){
		fprintf(stderr, "Usage: %s testcasefile\n", argv[0]);
		exit(1);
	}

	cfptr = (FILE *) fopen(argv[1], "r");
	int counter=0;

	while(fgets(test_case, sizeof(test_case)-1, cfptr)){
		//printf("line:%s",test_case);
		if( !strcmp(test_case,"\n") || !strcmp(test_case,"") )
			continue;
		//printf("Test case %d: %s", ++counter, test_case); fflush(stdout);
		process_line( test_case );
	}

      
    //Create FILE NOW academia.db
    strcat(handle.db_name,".db");
    FILE *ptr=fopen(handle.db_name,"ab+");
    fwrite(&handle,sizeof(struct PDS_DBInfo),1,ptr);
    fclose(ptr);
	return 0;
}
