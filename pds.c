#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "academia.h"
#include "pds.h"
#include"bst.h"

struct PDS_DB_Master db_handle;

//Local helper function
static int save_bst_in_file(struct BST_Node *root,struct PDS_RepoInfo *p);


int pds_db_open( char *db_name )
{
    if(db_handle.db_status==PDS_DB_OPEN)
        return PDS_DB_ALREADY_OPEN;
    else db_handle.db_status=PDS_DB_OPEN;

    char fileName[30];
    strcpy(fileName,db_name);
    strcat(fileName,".db");
    FILE *ptr=fopen(fileName,"rb");

    fread(&db_handle.db_info,sizeof(struct PDS_DBInfo),1,ptr);

    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        struct PDS_EntityInfo e=db_handle.db_info.entities[i];
        struct PDS_RepoInfo p;strcpy(p.entity_name,e.entity_name);
        p.entity_size=e.entity_size;
        p.pds_bst=NULL;
        char entityDataName[30];char entityIndexName[30];
        strcpy(entityDataName,p.entity_name);strcat(entityDataName,".dat");
        strcpy(entityIndexName,p.entity_name);strcat(entityIndexName,".ndx");
        p.pds_data_fp=fopen(entityDataName,"ab+");
        p.pds_ndx_fp=fopen(entityIndexName,"ab+");
        db_handle.entity_info[i]=p;

        pds_open(p.entity_name,p.entity_size);
    }
    for(int i=0;i<db_handle.db_info.num_relationships;i++)
    {
        struct PDS_LinkInfo r=db_handle.db_info.links[i];
        struct PDS_LinkFileInfo p;strcpy(p.link_name,r.link_name);
        char relFileName[100];
        strcpy(relFileName,r.pds_name1);strcat(relFileName,"_");strcat(relFileName,r.pds_name2);strcat(relFileName,".lnk");
        p.pds_link_fp=fopen(relFileName,"ab+");
        db_handle.rel_info[i]=p;
    }
    return PDS_SUCCESS;
}

void process_lines( char *test_case ,struct PDS_DBInfo *handle)
{	
	char dataType[30];
    char entityName[30];int entityRecordSize;
    char relationShipName[30];char entityOne[30];char entityTwo[30];
    sscanf(test_case,"%s ",dataType);
    if(strcmp(dataType,"entity")==0)
    {
        char redundant[30];
        sscanf(test_case,"%s %s %d",redundant,entityName,&entityRecordSize);
        struct PDS_EntityInfo e;
        strcpy(e.entity_name,entityName);
        e.entity_size=entityRecordSize;
        handle->entities[handle->num_entities]=e;
        handle->num_entities++;
    }
    else if(strcmp(dataType,"relationship")==0)
    {
        char redundant[30];
        sscanf(test_case,"%s %s %s %s",redundant,relationShipName,entityOne,entityTwo);
        struct PDS_LinkInfo r;
        strcpy(r.link_name,relationShipName);
        strcpy(r.pds_name1,entityOne);
        strcpy(r.pds_name2,entityTwo);
        handle->links[handle->num_relationships]=r;
        handle->num_relationships++;
    }
    else
    {
        strcpy(handle->db_name,dataType);
    }
}

int pds_create_schema (char *schema_file_name)
{
    char test_case[50];
    FILE *cfptr = (FILE *) fopen(schema_file_name, "r");
	int counter=0;
    
	while(fgets(test_case, sizeof(test_case)-1, cfptr)){
		//printf("line:%s",test_case);
		if( !strcmp(test_case,"\n") || !strcmp(test_case,"") )
			continue;
		//printf("Test case %d: %s", ++counter, test_case); fflush(stdout);
		process_lines( test_case ,&db_handle.db_info);
	}
    
    //Create FILE NOW academia.db
    //printf("FILECREATION\n");
    strcat(db_handle.db_info.db_name,".db");
    //printf("%s\n",handle.db_name);
    FILE *ptr=fopen(db_handle.db_info.db_name,"ab+");
    fwrite(&db_handle.db_info,sizeof(struct PDS_DBInfo),1,ptr);
    fclose(ptr);
}


int pds_open( char *repo_name, int rec_size )
{
    char dataFileName[30];
    char indexFileName[30];
    
    strcpy(dataFileName,repo_name);
    strcat(dataFileName,".dat");
    strcpy(indexFileName,repo_name);
    strcat(indexFileName,".ndx");
  
    FILE *pds_data_fp=fopen(dataFileName,"ab+");
    FILE *pds_ndx_fp=fopen(indexFileName,"ab+");

    if(pds_ndx_fp!=NULL)
    {
        for(int i=0;i<db_handle.db_info.num_entities;i++)
        {
            struct PDS_RepoInfo *p;
            p=&db_handle.entity_info[i];
            if(strcmp(p->entity_name,repo_name)==0)
            {
                pds_load_ndx(p);
                p->repo_status=PDS_ENTITY_OPEN;
                for(int i=0;i<100;i++)
                {
                    p->free_list[i]=-1;
                }
                break;
            }
        }
        fclose(pds_ndx_fp);//Close The indexFile
    }
    return PDS_SUCCESS;
}

int pds_load_ndx(struct PDS_RepoInfo *repo_handle)
{
    fseek(repo_handle->pds_ndx_fp,0,SEEK_SET);
    while(1)
    {
        struct PDS_NdxInfo *index = (struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
        int status = fread( index, sizeof(struct PDS_NdxInfo), 1, repo_handle->pds_ndx_fp );
        if(status == 0)
          break;
        bst_add_node(&repo_handle->pds_bst, index->key, index);
    }
}

int put_rec_by_key( char *entity_name, int key, void *rec )
{
    char dataFileName[30];
    strcpy(dataFileName,entity_name);
    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        struct PDS_RepoInfo *p;
        p=&db_handle.entity_info[i];
        if(strcmp(p->entity_name,dataFileName)==0)
        {
            struct PDS_NdxInfo *index = ( struct PDS_NdxInfo* )malloc( sizeof( struct PDS_NdxInfo ) );
            int offset, write_status, bst_insert_status;

            fseek(p->pds_data_fp, 0, SEEK_END );
            offset = ftell(p->pds_data_fp);//OFFSET FOR EOF

            fwrite(&key, sizeof(int),1, p->pds_data_fp);
            write_status = fwrite(rec,p->entity_size,1,p->pds_data_fp);
            if( write_status == 1 )
            {
              // insert index in bst
              index->key = key;
              index->offset = offset;

              bst_insert_status = bst_add_node(&p->pds_bst, key, index );
              if( bst_insert_status == BST_SUCCESS )
              {
                return PDS_SUCCESS;
              }
              else 
                return bst_insert_status;
            }
            else 
              return PDS_ADD_FAILED;    
        }
    }
}

int get_rec_by_ndx_key( char *entity_name, int key, void *rec )
{
    char dataFileName[30];
    strcpy(dataFileName,entity_name);

    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        struct PDS_RepoInfo *p;
        p=&db_handle.entity_info[i];
        if(strcmp(p->entity_name,dataFileName)==0)
        {
            //printf("The file is found\n");
            if(p->repo_status=PDS_ENTITY_OPEN)
            {
                int pk;//Primary key for record
                struct BST_Node *index;
                index = bst_search( p->pds_bst, key );
                if(index != NULL)
                {
                    //printf("Index is not null\n");
                    struct PDS_NdxInfo *temp;
                    temp =(struct PDS_NdxInfo*)index->data;
                    int checkIfExists=1;
                    for(int i=0;i<100;i++)
                    {
                        if(temp->offset==p->free_list[i] && p->free_list[i]!=-1)
                        {
                            //printf("Record deleted\n");
                            checkIfExists=0; //Record is deleted
                            //return PDS_REC_NOT_FOUND;
                            break;
                        }
                    }
                    if(checkIfExists==1)//Record is not deleted
                    {
                        //printf("Record found\n");
                        fseek(p->pds_data_fp, temp->offset, SEEK_SET);
                        fread( &pk,  sizeof(int), 1, p->pds_data_fp );
                        int status = fread(rec,p->entity_size,1, p->pds_data_fp );

                        if( status == 1 )
                        {
                            //printf("Success\n");
                            return PDS_SUCCESS;
                        }
                    }
                }
                return PDS_REC_NOT_FOUND;
            }else PDS_FILE_ERROR;
        }
    }
}

int get_rec_by_non_ndx_key( 
char *entity_name, /* The entity file from which data is to be read */
void *key,  			/* The search key */
void *rec,  			/* The output record */
int (*matcher)(void *rec, void *key), /*Function pointer for matching*/
int *io_count  		/* Count of the number of records read */
){}



int update_by_key( char *entity_name, int key, void *rec )
{
    char dataFileName[30];
    strcpy(dataFileName,entity_name);

    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        struct PDS_RepoInfo *p;
        p=&db_handle.entity_info[i];
        if(strcmp(p->entity_name,dataFileName)==0)
        {
            int write_status;
            if(p->repo_status==PDS_ENTITY_OPEN)
            {
                //declare index and contact variables
                struct BST_Node *index;
                //find index from bst using key
                index = bst_search(p->pds_bst,key);
                if( index != NULL )
                {
                    struct PDS_NdxInfo *temp;
                    temp =(struct PDS_NdxInfo*)index->data;
                    //temp = index->data;
                    // go to offset in data file
                    fseek(p->pds_data_fp, temp->offset, SEEK_SET);
                    fwrite(&key,sizeof(int),1,p->pds_data_fp);
                    write_status = fwrite(rec,p->entity_size,1,p->pds_data_fp);

                    if(write_status==1)
                      return PDS_SUCCESS;
                }
                else
                  return PDS_REC_NOT_FOUND;
            }
            else
              return PDS_FILE_ERROR;
        }
    }
}

int delete_by_key( char *entity_name, int key )
{
    char dataFileName[30];
    strcpy(dataFileName,entity_name);

    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        struct PDS_RepoInfo *p;
        p=&db_handle.entity_info[i];
        if(strcmp(p->entity_name,dataFileName)==0)
        {
            if(p->repo_status==PDS_ENTITY_OPEN)
            {
              struct BST_Node *index;
              struct PDS_NdxInfo *temp;

              index = bst_search(p->pds_bst, key );
              if(index != NULL)
              {
                  temp =(struct PDS_NdxInfo*)index->data;
                  //temp = index->data;
                  int checkIfExists=1;
                  for(int i=0;i<100;i++)
                  {
                      if(temp->offset==p->free_list[i])
                      {
                          checkIfExists=0; //Record is deleted
                          break;
                      }
                  }
                  if(checkIfExists==1)//Record is not deleted
                  {
                    //So deleted Here
                    checkIfExists=0;
                    //ADD TO FREE LIST
                    for(int i=0;i<100;i++)
                    {
                        if(p->free_list[i]==-1)
                        {
                            p->free_list[i]=temp->offset;
                            break;
                        }
                    }
                    //DELETE THE KEY IN BST
                    bst_del_node(&p->pds_bst,index->key);
                    return PDS_SUCCESS;
                  }
              }else return PDS_DELETE_FAILED;
            }else return PDS_FILE_ERROR;
        }
    }
}

int link_data(char *link_name, int key, int linked_key){}

int get_linked_data( char *link_name, int data_key, struct PDS_LinkedKeySet *linked_data ){}



int pds_close( char *entity_name )
{
    char dataFileName[30];
    strcpy(dataFileName,entity_name);

    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        struct PDS_RepoInfo *p;
        p=&db_handle.entity_info[i];
        if(strcmp(p->entity_name,dataFileName)==0)
        {
            for(int i=0;i<100;i++)
            {
                p->free_list[i]=-1;
            }
            if(p->repo_status==PDS_ENTITY_OPEN)
            {
                char dataFileName[30];
                char indexFileName[30];
                strcpy(dataFileName,p->entity_name);
                strcat(dataFileName,".dat");
                strcpy(indexFileName,p->entity_name);
                strcat(indexFileName,".ndx"); 

                //Open Index File
                p->pds_ndx_fp = fopen(indexFileName,"wb");
                fseek(p->pds_ndx_fp,0,SEEK_SET);

                if(p->pds_ndx_fp != NULL)
                {
                    //save bst in file
                    int bst_status = save_bst_in_file(p->pds_bst,p);
                    if( bst_status == 1 )
                    {
                          bst_free(p->pds_bst );
                          p->pds_bst = NULL;

                          int status1 = fclose(p->pds_data_fp);
                          int status2 = fclose(p->pds_ndx_fp);
                          if(status1 == 0 && status2 == 0)
                          {
                            p->repo_status = PDS_ENTITY_CLOSED;
                            return PDS_SUCCESS;
                          }
                    }
                }
            }else PDS_FILE_ERROR;
        }
    }
}

int pds_db_close()
{
    db_handle.db_status=PDS_DB_CLOSED;
    for(int i=0;i<db_handle.db_info.num_entities;i++)
    {
        char name[30];
        strcpy(name,db_handle.db_info.entities[i].entity_name);
        pds_close(name);
    }
    return PDS_SUCCESS;
}

static int save_bst_in_file(struct BST_Node *root ,struct PDS_RepoInfo *p)
{
    if(root == NULL)
      return 1;

    fseek(p->pds_ndx_fp, 0, SEEK_END);
    struct PDS_NdxInfo *index = (struct PDS_NdxInfo* )root->data;
    if(index != NULL)
    {
        fwrite( index, sizeof(struct PDS_NdxInfo), 1,p->pds_ndx_fp );
    }
    save_bst_in_file(root->left_child,p);
    save_bst_in_file(root->right_child,p);
}