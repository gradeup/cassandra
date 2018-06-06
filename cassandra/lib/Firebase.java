package org.apache.cassandra.triggers;

import java.util.HashMap;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.*;
import java.io.FileInputStream;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.google.firebase.FirebaseApp;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.json.JSONException;
import org.slf4j.LoggerFactory;
import org.json.simple.JSONObject;
import org.elasticsearch.action.get.GetResponse;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.io.util.FileUtils;

import java.io.InputStream;

import com.google.firebase.FirebaseOptions;
import com.google.firebase.FirebaseOptions.Builder;
import com.google.firebase.database.*;
import com.google.firebase.tasks.OnCompleteListener;
import com.google.firebase.tasks.Task;
import com.google.firebase.*;

import java.io.FileNotFoundException;

public class Firebase {
	private static final Logger logger = LoggerFactory.getLogger(Firebase.class);
  Memcached memcache;
  FirebaseDatabase db;

  static Firebase firebase;
  public static Firebase getInstance(){
      if(firebase==null){
      logger.info("ssooeee");
        firebase=new Firebase();
      }
      return firebase;
  }

	private Firebase() {
	 
		FileInputStream fs = null;
		memcache = new Memcached();
		try {
			fs = new FileInputStream(
					"/Users/gradeup/ashutosh/udofy-test-1cbf8761921d.json");
		} catch (FileNotFoundException ex) {
			logger.info("error file not found");
		}

		try {
			
			FirebaseOptions options = new FirebaseOptions.Builder()
					.setServiceAccount(fs)
					.setDatabaseUrl("https://udofy-test.firebaseio.com/")
					.build();
			logger.info("options" + options);
      List<FirebaseApp> list=FirebaseApp.getApps();
      if(list!=null&&list.size()>0){
        return;
      }
			FirebaseApp.initializeApp(options);
		} catch (Exception ex) {
			logger.info("exception", ex);
			ex.printStackTrace();
			logger.info("error file not found" + ex.getMessage());
		}
	}
	private DatabaseReference getReference(String path){
		DatabaseReference ref = FirebaseDatabase.getInstance().getReference(path);
				return ref;
	}

	private String getCurrentBucketName(){
		Calendar cal = Calendar.getInstance();
  	int month= cal.get(Calendar.MONTH)+1;
  	int date = cal.get(Calendar.DATE);
  	int year = cal.get(Calendar.YEAR);
  	String dateMap="";
  	if(date >0 && date <11){
  		dateMap="1";
  	}
  	if(date >10 && date<21){
  		dateMap="2";
  	}
  	if(date >20 && date<33){
  		dateMap="3";
  	}
  	return("bucket-" + month + "-" + dateMap +"-"+year);
	}

	private String getPreviousBucketName(){
		Calendar cal = Calendar.getInstance();
  	int month= cal.get(Calendar.MONTH)+1;
  	int date = cal.get(Calendar.DATE);
  	int year = cal.get(Calendar.YEAR);
  	int mm=month;
  	int yy=year;
  	String dateMap="";
  	if(date >0 && date <11){
  		dateMap="3";
  		mm=month-1;
  	}
  	if(date >10 && date<21){
  		dateMap="1";
  	}
  	if(date >20 && date<33){
  		dateMap="2";
  	}
  	if(month==1){
  		mm=12;
  		yy=year-1;
  	}
  	return("bucket-" + mm + "-" + dateMap +"-"+yy);
	}


  public void update(String tableName,HashMap<Object, Object> dataMap,Map<String, Object> partitionKeyData,String clusterName) {

    if(tableName.equals("post_comment")){       
      Object bucket=memcache.getCache("Firebase-bucketName-"+""+dataMap.get("postid"));
      String bucketName=null;
      if(bucket!=null){
        bucketName=String.valueOf(bucket);
      }
      if(bucket==null){
      	ElasticSearch es= new ElasticSearch(clusterName);
      	bucketName=getBucketNameFromEs(es,""+dataMap.get("postid"));
      	es.close();
      }
      String currentBucketName = getCurrentBucketName();
		  String previousBucketName = getPreviousBucketName();
      if(bucketName==null || bucketName=="old_post" ||  (!currentBucketName.equals(bucketName) && !previousBucketName.equals(bucketName))){
      	return; 
    	}
    	String path = "post_comment/" + currentBucketName + "/"+dataMap.get("postid") + "/" + dataMap.get("commentid");    	
    	try{
    	  DatabaseReference ref = getReference(tableName);
    	  if(ref==null){
    	  	return;
    	  }
    	  if(dataMap.get("spam")!=null){
    	  	ref=ref.child(bucketName).child(""+dataMap.get("postid")).child(""+dataMap.get("commentid")).child("spam");
    	  	ref.setValue(dataMap.get("spam"));
          return;
    	  }
    	  ref=ref.child(bucketName).child(""+dataMap.get("postid")).child(""+dataMap.get("commentid"));
    	  if(ref==null){
    	  	return;
    	  }
				dataMap.put("postid",""+dataMap.get("postid"));
				dataMap.put("commentid",""+dataMap.get("commentid"));
				dataMap.put("authorid",""+dataMap.get("authorid"));
				String stringToConvert = String.valueOf(dataMap.get("createdon"));
        Long convertedLong = Long.parseLong(stringToConvert)*-1;
        logger.info("comment posted"+ref);
    	  ref.setValue( dataMap);
      }
      catch(Exception ex){
      	logger.info("exception", ex);
				ex.printStackTrace();
				return;
      }
    }
    if(tableName.equals("comment_counts")){
    	Object bucket=memcache.getCache("Firebase-bucketName-"+""+partitionKeyData.get("commentid"));
    	String postId=""+memcache.getCache("Firebase-postId-"+""+partitionKeyData.get("commentid"));
      String bucketName=null;
      if(bucket!=null){
        bucketName=String.valueOf(bucket);
      }
    	if(bucket==null || postId==null){
    		try{
		  		ElasticSearch es = new ElasticSearch(clusterName);
		  		if(es==null){
		  			return;
		  		}
		    	postId=getPostIdFromEs(es,""+partitionKeyData.get("commentid"));
		    	bucketName=getBucketNameFromEs(es,postId);
		      es.close();
		      if(bucketName==null || postId==null){
		    		return;
		    	}
		    	if(bucketName.equals("old_post")){
		    		return;
		    	}
		    }
		    catch(Exception ex){
      		logger.info("exception", ex);
					ex.printStackTrace();
					return;
		    }
    	}
    	
    	logger.info("bucketName"+bucketName);
    	try{
    	  DatabaseReference ref = getReference("post_comment");
    	  if(ref==null){
    	  	return;
    	  }
    	  final DatabaseReference commentLikeRef=ref.child(bucketName).child(postId).child(""+partitionKeyData.get("commentid")).child("likecount");
				String stringToConvert = String.valueOf(dataMap.get("likecount"));
        Long likeCount = Long.parseLong(stringToConvert);
				commentLikeRef.addListenerForSingleValueEvent(new ValueEventListener() {
          @Override
          public void onDataChange(DataSnapshot dataSnapshot) {
            logger.info(""+dataSnapshot.getValue());
            long val ;
            if(dataSnapshot.getValue()==null){
            	if(likeCount==-1){
            		return;
            	}
            	val=1;
            }
            else{
            	val= Long.parseLong(""+dataSnapshot.getValue())+likeCount;
            }
            commentLikeRef.setValue(val);
          }
          @Override
          public void onCancelled(DatabaseError databaseError) {
            logger.info("Error in updating commentcount"+ databaseError);
          }
        });
      }
      catch(Exception ex){
      	logger.info("exception", ex);
			  ex.printStackTrace();
      }
    }
    if(tableName.equals("comment_user_actions")){
    	Object bucket=memcache.getCache("Firebase-bucketName-"+""+partitionKeyData.get("commentid"));	
    	String postId=""+memcache.getCache("Firebase-postId-"+""+partitionKeyData.get("commentid"));
      String bucketName=null;
      if(bucket!=null){
        bucketName=String.valueOf(bucket);
      }
    	if(bucket==null || postId==null){
    		try{
		  		ElasticSearch es = new ElasticSearch(clusterName);
		  		if(es==null){
		  			return;
		  		}
          if(postId==null){
		    	  postId=getPostIdFromEs(es,""+partitionKeyData.get("commentid"));
          }
		    	bucketName=getBucketNameFromEs(es,postId);
		    	if(bucketName==null || postId==null){
		    		return;
		    	}
		    	if(bucketName.equals("old_post")){
		    		return;
		    	}
		      es.close();
		    }
		    catch(Exception ex){
      		logger.info("exception", ex);
					ex.printStackTrace();
					return;
		    }
    	}

    	try{
    	  DatabaseReference ref = getReference("comment_user_actions");
    	  if(ref==null){
    	  	return;
    	  }
    	  ref=ref.child(bucketName).child(postId).child(""+dataMap.get("userid")).child(""+dataMap.get("commentid"));
    	  HashMap<String, Object> newDataMap = new HashMap<String, Object>();
				dataMap.put("type",""+dataMap.get("type"));
        if(dataMap.get("type").equals("like")){
          newDataMap.put("username",dataMap.get("username"));
        	newDataMap.put("isLikedByMe",true);
        }
        if(dataMap.get("type").equals("reported")){
        	newDataMap.put("isReportedByMe",true);
        }
				ref.setValue(newDataMap);
      }
      catch(Exception ex){
      	logger.info("exception", ex);
			  ex.printStackTrace();
      }
    }
	}

	public void delete(String tableName,Map<String, Object> partitionKeyData,Map<String, Object> clusterKeyData,String clusterName) {
		if(tableName.equals("post_comment")){     
		  Object bucket=memcache.getCache("Firebase-bucketName-"+""+clusterKeyData.get("commentid"));
      String bucketName=String.valueOf(bucket);
      logger.info("bucket"+bucket);
		  if(bucket==null){
    		try{
		  		ElasticSearch es = new ElasticSearch(clusterName);
		  		if(es==null){
		  			return;
		  		}
		    	String postId=""+partitionKeyData.get("postid");
		    	bucketName=getBucketNameFromEs(es,postId);
		      es.close();
		    }
		    catch(Exception ex){
      		logger.info("exception", ex);
					ex.printStackTrace();
					return;
		    }
    	}  
		  //get from es if bucketName is null
      logger.info("bucketName"+bucket);
		  String currentBucketName = getCurrentBucketName();
		  String previousBucketName = getPreviousBucketName();
      if(bucketName==null || bucketName=="old_post" ||  (!currentBucketName.equals(bucketName) && !previousBucketName.equals(bucketName))){
      	return; 
    	}
    	try{
    	  DatabaseReference ref = getReference(tableName);
    	  if(ref==null){
    	  	return;
    	  }
    	  ref=ref.child(bucketName).child(""+partitionKeyData.get("postid")).child(""+clusterKeyData.get("commentid"));
        if(ref==null){
    	  	return;
    	  }
    	  ref.setValue(null);
    	  return;
		  }
		  catch(Exception ex){
      	logger.info("exception", ex);
			  ex.printStackTrace();
			  return;
      }
	  }
	  if(tableName.equals("group_post")){
	  	String postId=""+clusterKeyData.get("postid");
	  	Object bucket=memcache.getCache("Firebase-bucketName-"+postId);
      String bucketName=null;
      if(bucket!=null){
        bucketName=String.valueOf(bucket);
      }
      if(bucket==null){
    		try{
		  		ElasticSearch es = new ElasticSearch(clusterName);
		  		if(es==null){
		  			return;
		  		}		  
		    	bucketName=getBucketNameFromEs(es,postId);
		      es.close();
          if(bucketName==null){
            return;
          }
		    }
		    catch(Exception ex){
      		logger.info("exception", ex);
					ex.printStackTrace();
					return;
		    }
    	}  
		  String currentBucketName = getCurrentBucketName();
		  String previousBucketName = getPreviousBucketName();
      if(bucketName==null || bucketName=="old_post" ||  (!currentBucketName.equals(bucketName) && !previousBucketName.equals(bucketName))){
      	return; 
    	}
    	try{
    	  DatabaseReference ref = getReference("post_comment");
    	  if(ref==null){
    	  	return;
    	  }
        logger.info("12");
    	  ref=ref.child(bucketName).child(postId);
        if(ref==null){
    	  	return;
    	  }
				ref.setValue(null);
      }
      catch(Exception ex){
      	logger.info("exception", ex);
			  ex.printStackTrace();
      }
	  }
	}


	public String getBucketNameFromEs(ElasticSearch es,String postId){
		try{	
    	GetResponse getResponse = es.getDocument("group_post","group_post",postId,true);
    	if(getResponse==null){
  			return null;
  		}
    	Map<String, Object> postSource = getResponse.getSource();
    	if(postSource==null){
    		return null;
      }
      String bucketName=""+postSource.get("bucketname");
    	return bucketName;
    }
    catch(Exception ex){
  		logger.info("exception", ex);
			ex.printStackTrace();
			return null;
    }
	}

	public String getPostIdFromEs(ElasticSearch es,String commentId){
		try{	
  		GetResponse getResponse = es.getDocument("post_comment","post_comment",commentId,true);
  		if(getResponse==null){
  			return null;
  		}
    	Map<String, Object> commentSource = getResponse.getSource();
    	if(commentSource==null){
    		return null;
      }
    	String postId=""+commentSource.get("postid");
    	return postId;
    }
    catch(Exception ex){
  		logger.info("exception", ex);
			ex.printStackTrace();
			return null;
    }
	}
}
