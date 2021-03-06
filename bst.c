#include <stdlib.h>
#include <stdio.h>
#include "bst.h"

// Local functions
static int place_bst_node( struct BST_Node *parent, struct BST_Node *node );
static struct BST_Node *make_bst_node( int key, void *data );
static struct BST_Node * minValueNode(struct BST_Node* node);
static struct BST_Node* deleteNode(struct BST_Node* root, int key);

// Root's pointer is passed because root can get modified for the first node
int bst_add_node( struct BST_Node **root, int key, void *data )
{
	struct BST_Node *newnode = NULL;
	struct BST_Node *parent = NULL;
	struct BST_Node *retnode = NULL;
	int status = 0;

	newnode = make_bst_node( key, data);
	if( *root == NULL ){
		*root = newnode;
		status = BST_SUCCESS;
	}
	else{
		status = place_bst_node( *root, newnode );
	}
	return status;
}

struct BST_Node *bst_search( struct BST_Node *root, int key )
{
	struct BST_Node *retval = NULL;

	if( root == NULL ){
		return NULL;
	}
	else if( root->key == key )
		return root;
	else if( key < root->key )
		return bst_search( root->left_child, key );
	else if( key > root->key )
		return bst_search( root->right_child, key );
}
void bst_print( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		printf("%d ", root->key);
		bst_print( root->left_child );
		bst_print( root->right_child );
	}
}

void bst_free( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root);
	}
}

void bst_destroy( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root->data);
		free(root);
	}
}

static int place_bst_node( struct BST_Node *parent, struct BST_Node *node )
{
	int retstatus;

	if( parent == NULL ){
		return BST_NULL;
	}
	else if( node->key == parent->key ){
		return BST_DUP_KEY;
	}
	else if( node->key < parent->key ){
		if( parent->left_child == NULL ){
			parent->left_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->left_child, node );
		}
	}
	else if( node->key > parent->key ){
		if( parent->right_child == NULL ){
			parent->right_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->right_child, node );
		}
	}
}

static struct BST_Node *make_bst_node( int key, void *data )
{
	struct BST_Node *newnode;
	newnode = (struct BST_Node *) malloc(sizeof(struct BST_Node));
	newnode->key = key;
	newnode->data = data;
	newnode->left_child = NULL;
	newnode->right_child = NULL;

	return newnode;
}

int bst_del_node( struct BST_Node **root, int key )
{
    deleteNode(*root,key);
	return BST_SUCCESS;
}

struct BST_Node * minValueNode(struct BST_Node* node) 
{ 
    struct BST_Node* current = node; 
  
    /* loop down to find the leftmost leaf */
    while (current && current->left_child != NULL) 
        current = current->left_child; 
  
    return current; 
} 
struct BST_Node* deleteNode(struct BST_Node* root, int key) 
{ 
    // base case 
    if (root == NULL) return root; 
  
    //If node to be deleted lies in leftSubtree 
    if (key < root->key) 
        root->left_child = deleteNode(root->left_child, key); 
  
    //If node to be deleted lies in rightSubtree
    else if (key > root->key) 
        root->right_child = deleteNode(root->right_child, key); 
  
    //We got the node to be deleted
    else
    { 
        if (root->left_child == NULL) 
        { 
            struct BST_Node *temp = root->right_child; 
            free(root); 
            return temp; 
        } 
        else if (root->right_child == NULL) 
        { 
            struct BST_Node *temp = root->left_child; 
            free(root); 
            return temp; 
        } 
        else
        {
            struct BST_Node* temp = minValueNode(root->right_child); 
            //leftMost child in right subtree
            root->key = temp->key; 
            root->data=temp->data;
            // Delete the inorder successor 
            root->right_child = deleteNode(root->right_child, temp->key); 
        }
    } 
    return root; //Returns root of the tree
} 