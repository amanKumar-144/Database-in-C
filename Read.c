#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "academia.h"
#include "pds.h"

int main(int argc, char *argv[])
{
    FILE *ptr; ptr=fopen("academia.db","rb");
    struct PDS_DBInfo d;
    fread(&d,sizeof(struct PDS_DBInfo),1,ptr);
//struct PDS_DBInfo{
//	char db_name[30];
//	int num_entities;
//	struct PDS_EntityInfo entities[MAX_ENTITY];
//	int num_relationships;
//	struct PDS_LinkInfo links[MAX_RELATIONSHIPS];
//};

    printf("The name of file is %s\n",d.db_name);
    printf("The total entities are %d\n",d.num_entities);
    printf("The total relationships are %d\n",d.num_relationships);
	return 0;
}
