#include<stdio.h>
#include<string.h>
#include<errno.h>
#include<stdlib.h>
#include "pds.h"
#include "bst.h"
//#include"contact.h" //FOR DEBUGGING

// Define the global variable
struct PDS_RepoInfo repo_handle;
void build_bst();
static int save_bst_in_file(struct BST_Node *root);
static void set_file_names( char *repo_name, char *data_file_name, char *index_file_name );

int pds_open( char *repo_name, int rec_size )
{
  char dataFileName[30];
  char indexFileName[30];
  if(repo_handle.repo_status==PDS_REPO_OPEN)
      return PDS_REPO_ALREADY_OPEN;
  
  repo_handle.rec_size=rec_size;
  repo_handle.pds_bst=NULL;//Every Time we open pds we re-initialize bst;
  
  strcpy(repo_handle.pds_name,repo_name);
  strcpy(dataFileName,repo_name);
  strcat(dataFileName,".dat");
  strcpy(indexFileName,repo_name);
  strcat(indexFileName,".ndx");
  

  repo_handle.pds_data_fp=fopen(dataFileName,"ab+");
  repo_handle.pds_ndx_fp=fopen(indexFileName,"ab+");
  repo_handle.repo_status=PDS_REPO_OPEN;
  if(repo_handle.pds_ndx_fp!=NULL)
  {
      pds_load_ndx();
      fclose(repo_handle.pds_ndx_fp);//Close The indexFile
  }
  return PDS_SUCCESS;
}


int get_rec_by_ndx_key( int key, void *rec )
{
  if( repo_handle.repo_status == PDS_REPO_OPEN )
  {
    int pk;//Primary key for record
    struct BST_Node *index;

    
    index = bst_search( repo_handle.pds_bst, key );
    if( index != NULL )
    {
      struct PDS_NdxInfo *temp;
      temp =(struct PDS_NdxInfo*)index->data;
      int checkIfExists=1;
      for(int i=0;i<100;i++)
      {
          if(temp->offset==repo_handle.free_list[i])
          {
              checkIfExists=0; //Record is deleted
              break;
          }
      }
      if(checkIfExists==1)//Record is not deleted
      {
        fseek(repo_handle.pds_data_fp, temp->offset, SEEK_SET);
        fread( &pk,  sizeof(int), 1, repo_handle.pds_data_fp );
        int status = fread( rec,  repo_handle.rec_size , 1, repo_handle.pds_data_fp );

        if( status == 1 )
          return PDS_SUCCESS;
      }
    }
    return PDS_REC_NOT_FOUND;
  }
  else
    return PDS_FILE_ERROR;
}

int get_rec_by_non_ndx_key(void *key,void *rec,int (*matcher)(void *rec, void *key),int *io_count)
{
  struct BST_Node *index;
  int count=0;
  int pk;
  if( repo_handle.repo_status == PDS_REPO_OPEN)
  {
    fseek(repo_handle.pds_data_fp, 0, SEEK_SET);
    while(1)
    {
        fread( &pk , sizeof(int), 1, repo_handle.pds_data_fp );
        int status = fread( rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp );
        if(status!= 1)
          break;
        count=count+1;
        //struct Contact *c=(struct Contact *)rec;
        //char *phoneKey=(char *)key;
        //printf("Hello The key is %s",phoneKey);
        //printf("Hello The data is %d %s %s",(c->contact_id),c->contact_name,c->phone);
        if( matcher(rec, key) == 0)
        {
            index = bst_search( repo_handle.pds_bst, pk );//We use Key HERE
            if( index != NULL )
            {
                struct PDS_NdxInfo *temp;
                temp =(struct PDS_NdxInfo*)index->data;
                int checkIfExists=1;
                for(int i=0;i<100;i++)
                {
                    if(temp->offset==repo_handle.free_list[i])
                    {
                        checkIfExists=0; //Record is deleted already
                        break;
                    }
                }
                if(checkIfExists==1)//Record is not deleted
                {
                    //Remove redundant count
                    int deletes=0;
                    for(int i=0;i<100;i++)
                    {
                        if(repo_handle.free_list[i]<temp->offset && repo_handle.free_list[i]!=0)
                            deletes++;
                        if(repo_handle.free_list[i]==0)break;
                    }
                  *io_count = count-deletes;
                  return PDS_SUCCESS;
                }
            }
        }
    }
    return PDS_REC_NOT_FOUND;
  }
  else
    return PDS_FILE_ERROR;
}

int put_rec_by_key( int key, void *rec )
{
  if( repo_handle.repo_status == PDS_REPO_OPEN )
  {
    // declare index and status variables
    struct PDS_NdxInfo *index = ( struct PDS_NdxInfo* )malloc( sizeof( struct PDS_NdxInfo ) );
    int offset, write_status, bst_insert_status;

    
    fseek( repo_handle.pds_data_fp, 0, SEEK_END );
    offset = ftell(repo_handle.pds_data_fp);//OFFSET FOR EOF

    //Check in freelist if empty position is found
    for(int i=0;i<100;i++)
    {
      if(repo_handle.free_list[i]!=0)
      {
        offset=repo_handle.free_list[i];break; //So we Write new record in 1st free deleted position
      }
    }
    
    fwrite( &key, sizeof(int) , 1, repo_handle.pds_data_fp);
    write_status = fwrite( rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);
    if( write_status == 1 )
    {
      // insert index in bst
      index->key = key;
      index->offset = offset;
  

      bst_insert_status = bst_add_node( &repo_handle.pds_bst, key, index );
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
  else
    return PDS_FILE_ERROR;
}

int pds_close()
{
  if( repo_handle.repo_status == PDS_REPO_OPEN )
  {
    // set file names 
    char dataFileName[30];
    char indexFileName[30];
    
    strcpy(dataFileName,repo_handle.pds_name);
    strcat(dataFileName,".dat");
    strcpy(indexFileName,repo_handle.pds_name);
    strcat(indexFileName,".ndx");

    //open index file
    repo_handle.pds_ndx_fp = fopen( indexFileName, "wb" );
    fseek( repo_handle.pds_ndx_fp, 0, SEEK_SET );

    if( repo_handle.pds_ndx_fp != NULL)
    {
      //save bst in file
      int bst_status = save_bst_in_file( repo_handle.pds_bst );

      if( bst_status == 1 )
      {
        bst_free( repo_handle.pds_bst );
        repo_handle.pds_bst = NULL;

        int status1 = fclose(repo_handle.pds_data_fp);
        int status2 = fclose(repo_handle.pds_ndx_fp);

        if(status1 == 0 && status2 == 0)
        {
          repo_handle.repo_status = PDS_REPO_CLOSED;
          return PDS_SUCCESS;
        }
      }
    }
  }
  else
    return PDS_FILE_ERROR;
}

int update_by_key( int key, void *rec )
{
  int write_status;
  if( repo_handle.repo_status == PDS_REPO_OPEN )
  {
    // declare index and contact variables
    struct BST_Node *index;

    // find index from bst using key
    index = bst_search( repo_handle.pds_bst, key );
    if( index != NULL )
    {
      struct PDS_NdxInfo *temp;
      temp =(struct PDS_NdxInfo*)index->data;
      //temp = index->data;
      // go to offset in data file
      fseek(repo_handle.pds_data_fp, temp->offset, SEEK_SET);

      fwrite( &key, sizeof(int) , 1, repo_handle.pds_data_fp);
      write_status = fwrite( rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);

      if( write_status )
        return PDS_SUCCESS;
    }
    else
      return PDS_REC_NOT_FOUND;
  }
  else
    return PDS_FILE_ERROR;
}
int delete_by_key( int key )
{
  if( repo_handle.repo_status == PDS_REPO_OPEN )
  {
    struct BST_Node *index;
    struct PDS_NdxInfo *temp;

    index = bst_search( repo_handle.pds_bst, key );
    if(index != NULL)
    {
        temp =(struct PDS_NdxInfo*)index->data;
        //temp = index->data;
        int checkIfExists=1;
        for(int i=0;i<100;i++)
        {
            if(temp->offset==repo_handle.free_list[i])
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
              if(repo_handle.free_list[i]==0)
              {
                  repo_handle.free_list[i]=temp->offset;
                  break;
              }
          }
          //DELETE THE KEY IN BST
          bst_del_node(&repo_handle.pds_bst,index->key);
          return PDS_SUCCESS;
        }
    }
    return PDS_DELETE_FAILED;
  }
  else
    return PDS_FILE_ERROR;

}

//I am not including nodes in free-list to put correct BST
//Since LAB SUBMISSION REQUIRES ONLY 2 FILES TO SUBMIT  I AM USING FREE-LIST TO ADD CORRECT NODE TO BST
static int save_bst_in_file( struct BST_Node *root )
{
  if( root == NULL)
    return 1;

  fseek(repo_handle.pds_ndx_fp, 0, SEEK_END);
  struct PDS_NdxInfo *index = (struct PDS_NdxInfo* )root->data;

  if(index != NULL)
  {
  //    int checkIfExists=1;
  //    for(int i=0;i<100;i++)
  //    {
  //        if(index->offset==repo_handle.free_list[i])
  //        {
  //            checkIfExists=0;break;
  //        }
  //    }
  //    if(checkIfExists==1)
  //    {
        fwrite( index, sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp );
  //    }
  }
  save_bst_in_file( root->left_child );
  save_bst_in_file( root->right_child );
}

int pds_load_ndx()
{
  fseek( repo_handle.pds_ndx_fp, 0, SEEK_SET );
  while(1)
  {
    struct PDS_NdxInfo *index = (struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
    int status = fread( index, sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp );
    if( status == 0)
      break;
    bst_add_node( &repo_handle.pds_bst, index->key, index);
  }
}