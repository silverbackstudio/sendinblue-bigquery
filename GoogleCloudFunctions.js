const BigQuery = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const dataset = bigquery.dataset('email_events');

const table_transactional = dataset.table('transactional');
const table_campaigns = dataset.table('campaigns');

exports.insertTransactional = (funcReq, funcRes) => {
  console.log( funcReq.body ); 
  var eventData = funcReq.body;
  
  //BigQuery don't support dashes in column names, replace it with underscore.
  eventData.message_id = eventData["message-id"];
  delete eventData["message-id"]; 
  
  table_transactional.insert(eventData)
  	.then(function(data) {
		  console.log(data[0]);
  	})
    .catch(function(err) {
    
      if (err.name === 'PartialFailureError') { 
        for (var rowKey in err.errors) {
            console.error(JSON.stringify( err.errors[rowKey] ));
        }      
      } else {
        console.error(err);
      }
      
      funcRes.status(500).send(JSON.stringify( err ));
  });      
};

exports.insertCampaign = (funcReq, funcRes) => {
  console.log( funcReq.body ); 
  var eventData = funcReq.body;
  
  table_campaigns.insert(eventData)
  	.then(function(data) {
		  console.log(data[0]);
  	})
    .catch(function(err) {
    
      if (err.name === 'PartialFailureError') { 
        for (var rowKey in err.errors) {
            console.error(JSON.stringify( err.errors[rowKey] ));
        }      
      } else {
        console.error(err);
      }
    
      funcRes.status(500).send(JSON.stringify( err ));
  });
};
