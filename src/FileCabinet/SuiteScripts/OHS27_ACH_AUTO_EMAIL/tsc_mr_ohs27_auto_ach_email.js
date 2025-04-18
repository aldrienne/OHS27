/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/search', 'N/runtime', 'N/render', 'N/email', 'N/file', 'N/record'],

    (search, runtime, render, email, file, record) => {
        const SCRIPT_PARAM_ELIGIBLE_ACH_PAYMENTS_SEARCH = 'custscript_tsc_ohs27_eligible_ach_paymen';
        const SCRIPT_PARAM_EMAIL_AUTHOR = 'custscript_tsc_ohs27_email_author';
        const SCRIPT_PARAM_PRINT_TEMPLATE_ID = "custscript_tsc_ohs27_print_template_id";
        const VENDOR_PAYMENT_FIELD_EMAIL_SENT = "custbody_tsc_ach_auto_email_sent";
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            try {
                // Get all required parameters
                const searchId = runtime.getCurrentScript().getParameter({ name: SCRIPT_PARAM_ELIGIBLE_ACH_PAYMENTS_SEARCH });
                const printTemplateId = runtime.getCurrentScript().getParameter({ name: SCRIPT_PARAM_PRINT_TEMPLATE_ID });
                const authorId = runtime.getCurrentScript().getParameter({ name: SCRIPT_PARAM_EMAIL_AUTHOR });
                
                // Log parameters for debugging
                log.audit('Script Parameters', {
                    searchId: searchId,
                    printTemplateId: printTemplateId,
                    authorId: authorId
                });
                
                // Validate each parameter individually
                const missingParams = [];
                
                if (!searchId) missingParams.push('Eligible ACH Payments Search');
                if (!printTemplateId) missingParams.push('Print Template ID');
                if (!authorId) missingParams.push('Email Author');
                
                if (missingParams.length > 0) {
                    throw new Error(`Required script parameter(s) not configured: ${missingParams.join(', ')}`);
                }
                
                // Validate search exists and count records
                try {                    
                    const searchObj = search.load({ id: searchId });
                    
                    const resultCount = searchObj.runPaged().count;
                    
                    log.audit('Input Data Records', `Found ${resultCount} eligible payment records to process`);
                    
                    // Return the search object for processing
                    return searchObj;
                } catch (loadError) {
                    throw new Error(`Invalid search ID (${searchId}): ${loadError.message}`);
                }
            } catch (e) {
                log.error('Error in getInputData', e);
                throw e; // Re-throw to halt script execution when parameters aren't properly configured
            }
        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            try {
                // Log incoming data for debugging
                log.debug('Map Input', { key: mapContext.key, value: mapContext.value });

                // Parse the value once
                const searchResult = JSON.parse(mapContext.value);
                const values = searchResult.values;

                // Validate required fields
                if (!values.account || !values.account.value) {
                    throw new Error('Account value is missing in search result');
                }

                // Extract account ID (will be our output key)
                const accountId = values.account.value + "_" + values.entity.value;
                log.debug('Processing Account ID', accountId);

                // Build normalized order object with all needed fields
                const orderObj = {
                    orderId: mapContext.key,
                    accountId: accountId,
                    orderDate: values.trandate || '',
                    postingPeriod: values.postingperiod ? values.postingperiod.text : '',
                    orderNumber: values.tranid || '',
                    entity: values.entity ? values.entity.text : '',
                };

                // Pass to reduce stage grouped by account ID
                mapContext.write({
                    key: accountId,
                    value: orderObj
                });

            } catch (e) {
                log.error({
                    title: 'Error in map function',
                    details: `Order ID: ${mapContext.key}, Error: ${e.message}`
                });
            }
        }

        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {
            try {
                log.debug(reduceContext.key, reduceContext.values);
                let vendorId = reduceContext.key.split('_')[1];
                let accountId = reduceContext.key.split('_')[0];
                let emailTemplateId = searchRelatedEmailTemplate(accountId);
                log.debug('Email Template ID', emailTemplateId);
                let authorId = runtime.getCurrentScript().getParameter({ name: SCRIPT_PARAM_EMAIL_AUTHOR });
                log.debug('Email Author ID', authorId);

                let transactionsId = [];

                reduceContext.values.forEach((value) => {
                    let orderObj = JSON.parse(value);
                    transactionsId.push(orderObj.orderId);
                });
                log.debug('Transactions ID', transactionsId);

                // Generate individual payment vouchers
                let pdfFiles = generateIndividualPaymentVoucher(transactionsId);
                log.debug('Generated PDF Files', pdfFiles);

                //Merge Email
                var mergeResult = render.mergeEmail({
                    templateId: emailTemplateId,
                    entity: {
                        type: 'employee',
                        id: parseInt(authorId)
                    },
                    recipient: {
                        type: 'vendor',
                        id: parseInt(vendorId)
                    }
                });

                //Construct emailObj
                
                let emailObj = {
                    author: authorId,
                    recipients: vendorId,
                    subject: mergeResult.subject,
                    body: mergeResult.body,
                    attachments: pdfFiles,
                }

                log.debug('Email Object', emailObj);

                try{
                    email.send(emailObj);
                    //Update transactionids' status to 'Email Sent' true
                    transactionsId.forEach((transactionId) => {
                        let vendorPaymentRecord = record.load({
                            type: record.Type.VENDOR_PAYMENT,
                            id: transactionId,
                            isDynamic: true
                        });
                        vendorPaymentRecord.setValue({
                            fieldId: VENDOR_PAYMENT_FIELD_EMAIL_SENT,
                            value: true
                        });
                        vendorPaymentRecord.save();
                    });
                }catch(sendEmailError){
                    log.error('sendEmail', sendEmailError);                    
                }

            } catch (e) {
                log.error('reduce', e);
            }
        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {

        }

        const searchRelatedEmailTemplate = (accountId) => {
            try {
                // Create search for email template mapping
                const emailTemplateSearch = search.create({
                    type: 'customrecord_tsc_acct_email_template_map',
                    filters: [
                        ['custrecord_tsc_account', 'anyof', accountId]
                    ],
                    columns: ['custrecord_tsc_email_template_id']
                });

                // Run search and get results
                const searchResult = emailTemplateSearch.run().getRange({
                    start: 0,
                    end: 1
                });

                // Return template ID if found
                if (searchResult.length > 0) {
                    return searchResult[0].getValue('custrecord_tsc_email_template_id');
                } else {
                    throw new Error('No email template found for account ID: ' + accountId);
                }
            } catch (e) {
                log.error('searchRelatedEmailTemplate', e);
                throw e;
            }
        }

        const generateIndividualPaymentVoucher = (transactionIds) => {
            
            let pdfFiles = [];
        
            transactionIds.forEach((transactionId) => {
                try {
                    // Create renderer
                    let renderer = render.create();
                    
                    // Set template
                    renderer.setTemplateByScriptId({
                        scriptId: runtime.getCurrentScript().getParameter({ name: SCRIPT_PARAM_PRINT_TEMPLATE_ID })
                    });
                    
                    // Add record
                    renderer.addRecord({
                        templateName: 'record',
                        record: record.load({
                            type: 'vendorpayment',
                            id: transactionId
                        })
                    });
                    
                    // Render PDF
                    let pdfContent = renderer.renderAsPdf();
                    
                    // Create file record
                    let pdfFile = file.create({
                        name: 'ACH_Payment_' + transactionId + '.pdf',
                        fileType: file.Type.PDF,
                        contents: pdfContent.getContents(),
                        folder: 36472 // Your folder ID
                    });                
                    
                    // Add the file ID to the array
                    pdfFiles.push(pdfFile);                    
                } catch (e) {
                    log.error('Error generating PDF for transaction ' + transactionId, e);
                }
            });
            
            return pdfFiles;
        };

        const sendEmail = () =>{

        }


        return { getInputData, map, reduce, summarize }

    });
