const modules = global.modules;
const path = modules.require("path");
const { GenericScriptBase, constants, markerService, ruleService, cacheService, contentService, config } = modules;
const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

let modPath = require.resolve("./HttpHelper");
delete require.cache[modPath];
const HttpHelper = require(modPath);
modPath = require.resolve("./common");
delete require.cache[modPath];
const { GetAuthTokent, getHostDetails } = require(modPath);

const { htmlToText } = modules.require('html-to-text');
const { Client } = require("@microsoft/microsoft-graph-client");
const { ConfidentialClientApplication, } = require("@azure/msal-node");


class ReadMSEmailConnector extends GenericScriptBase {
  async process() {
    let self = this;

    let inputData = self.data;

    Object.keys(self.scriptParams).forEach(key => {
      inputData[key] = self.scriptParams[key];
    });

    self.schedule_run = true;
    self.fetchCount = self.scriptParams.fetchCount;
    self.processDefKey = self.scriptParams.processDefKey;
    self.automationKey = self.processKey + '-' + self.taskKey;
    try {

      let emailClient = await self.createEmailApiClient(inputData);
      let rslt = await self.read365Mail(emailClient, inputData);
      if (rslt.rc != 0) return rslt;

      return { rc: 0 };

    } catch (e) {
      return { rc: 1, msg: `Error: ${e.message || e.response}` };
    }
  }


  async createEmailApiClient(inputData) {

    let rslt = await cacheService.getRecord(inputData.ServiceAccountId);
    if (rslt.rc != 0) return rslt;

    const clientId = rslt.data.config_dat.client_id;
    const clientSecret = rslt.data.config_dat.client_password;
    const tenantId = rslt.data.config_dat.tenant_id;

    const scopes = ["https://graph.microsoft.com/.default"];

    const cca = new ConfidentialClientApplication({
      auth: {
        clientId,
        authority: `https://login.microsoftonline.com/${tenantId}`,
        clientSecret,
      },
    });

    const tokenResponse = await cca.acquireTokenByClientCredential({ scopes });
    const accessToken = tokenResponse.accessToken;

    const client = Client.init({
      authProvider: (done) => done(null, accessToken),
    });
    return client

  }

  HtmlTotext(html) {
    return htmlToText(html, { wordwrap: 130 });
  }
  parseBoolean(value) {
    if (typeof value === 'string') {
      value = value.trim().toLowerCase();
    }

    return value === true || value === 'true' || value === '1';
  }
  async read365Mail(client, inputData) {
    let self = this;
    let { automationKey, automationId, fetchCount, processDefKey } = self;

    let last_message_time_key = `${automationKey}-time_stamp`;
    let last_message_next_link = `${automationKey}-next_link`;

    let rslt = await markerService.getValue(automationKey);
    if (rslt.rc != 0) return rslt;
    let lastId = rslt.data || null;

    rslt = await markerService.getValue(last_message_time_key);
    if (rslt.rc != 0) return rslt;
    let lastId_timestamp = rslt.data || null;

    let lastId_next_link = await self.nextLink(null, "GET");

    self.lastRun = { lastId: lastId, ticketCount: 0, time: Date.now() };


    inputData.fetchUnreadOnly = self.parseBoolean(inputData.fetchUnreadOnly);
    inputData.readFreshNewEmailsOnly = self.parseBoolean(inputData.readFreshNewEmailsOnly);
    inputData.saveAttachments = self.parseBoolean(inputData.saveAttachments);


    try {
      let rslt = await cacheService.getRecord(inputData.ServiceProfileId);
      const userEmail = rslt.data.config_dat.tenant_user;
      const folderName = rslt.data.config_dat.mail_box;

      let mailFolders = await client.api(`/users/${userEmail}/mailFolders`).get();
      let mailFolder = null;
      if (mailFolders) {
        mailFolder = mailFolders.value.find(f => f.displayName.toLowerCase() === folderName.toLowerCase());
      }


      if (mailFolder && mailFolder.id) {
        const emailQuery = await client.api(`/users/${userEmail}/mailFolders/${mailFolder.id}/messages`)
          .select("id,subject,receivedDateTime,from,toRecipients,ccRecipients,bccRecipients,body,hasAttachments,isRead,internetMessageHeaders")
          // .orderby("receivedDateTime asc") // Sort by latest first
          .top(inputData.fetchCount);

        if (lastId_timestamp) {
          emailQuery.filter(`receivedDateTime ge ${lastId_timestamp}`);
        }

        if (inputData.fetchUnreadOnly) {
          emailQuery.filter("isRead eq false");
        }
        emailQuery.orderby("receivedDateTime asc") // Sort by latest first
        let emails = {};
        if (lastId_next_link) {
          emails = await client.api(lastId_next_link).get();
        }
        else {
          emails = await emailQuery.get();
        }

        let next_link = emails["@odata.nextLink"];
        let emailValues = emails.value;
        if (emailValues && emailValues.length > 0) {
          if (inputData.readFreshNewEmailsOnly) {
            emailValues = emailValues.filter(email => {
              let headers = email.internetMessageHeaders || [];
              const headerNames = headers.map(h => h.name.toLowerCase());
              return !headerNames.includes('in-reply-to') && !headerNames.includes('references');
            });
          }

          if (lastId) {
            emailValues = emailValues.filter(email => {
              let last_date = new Date(lastId_timestamp);
              let current_email_date = new Date(email.receivedDateTime);
              if (current_email_date > last_date) {
                return true;
              }
              else if (current_email_date == last_date) {
                return email.id > lastId;
              }
            });
          }

        }

        if (emailValues && emailValues.length > 0) {
          for (let emailData of emailValues) {

            let emailFields = {};
            emailFields.sender = emailData.from?.emailAddress?.address || null;
            emailFields.cc_email = emailData.ccRecipients.map(e => e.emailAddress.address).join(',');
            emailFields.bcc_email = emailData.bccRecipients.map(e => e.emailAddress.address).join(',');
            emailFields.message_uid = emailData.id;
            emailFields.subject = emailData.subject;
            emailFields.email_message = self.HtmlTotext(emailData.body.content);
            emailFields.received_date = new Date(emailData.receivedDateTime).getTime();

            if (inputData.saveAttachments && emailData.hasAttachments) {
              let attachmentsResponse = await client.api(`/users/${userEmail}/messages/${emailData.id}/attachments`).get();
              emailFields.attachments = [];
              for (const att of attachmentsResponse.value) {
                if (att['@odata.mediaContentType'] && att.contentBytes) {
                  const buffer = Buffer.from(att.contentBytes, 'base64');

                  emailFields.attachments.push({
                    category: constants.contentCategory.Inbound,
                    file_name: att.name,
                    content: buffer,
                    content_type: att.contentType
                  });

                } else {
                  console.warn(`Skipped unsupported attachment: ${att.name}`);
                }
              }
            }
            lastId = emailFields.message_uid;
            lastId_timestamp = emailData.receivedDateTime;

            rslt = await markerService.updateValue(automationKey, lastId);
            if (rslt.rc != 0) return rslt;

            rslt = await markerService.updateValue(last_message_time_key, lastId_timestamp);
            if (rslt.rc != 0) return rslt;
            //next_link

            if (typeof (inputData.custom_filter) == 'string' && inputData.custom_filter.trim() != '') {
              rslt = await self.execute_customFilter(inputData, emailFields);
              if (rslt.rc != 0) return rslt;

              if (rslt.data && self.parseBoolean(rslt.data.skip)) {
                continue;
              }

            }

            rslt = await self.createInstance(emailFields);
            if (rslt.rc != 0) return rslt;

            console.debug(automationKey + ' - Received mail from #' + emailFields.sender + '# messageUid ' + emailFields.message_uid);

            // try {
            // } catch (error) {
            //   return { rc: 1, msg: `Failed to mark email as read ${mail.id}: ${error.message}` };
            // }


          }

        }
        else {
          console.log("No new email found!");
        }
        if (next_link) {
          await self.nextLink(next_link, "UPDATE");
        }

        return { rc: 0 };
      }
      else {
        return { rc: 1, msg: `Provided mail folder "${folderName} is does not exists or removed.` };
      }

    } catch (error) {
      return { rc: 1, msg: error.message };
    }
  }

  async execute_customFilter(inputData, emailFields) {
    let script = await ruleService.buildRuleScript({ type: 4, config_dat: { script_file: inputData.custom_filter } });
    if (script.rc != 0) return script;
    let f = new script.data();
    let rslt = await f['init'](
      {
        'scriptParams': inputData,
        'data': emailFields
      });
    if (rslt.rc != 0) return rslt;
    rslt = await f['process']();

    return rslt;
  }
  async nextLink(nextLink, mode = "GET") {
    let filePath = `${config.folders.data.data_attachments}/qms_read_email_nextlink.txt`;
    if (mode == "GET") {
      if (!fs.existsSync(filePath))
        return "";
      return fs.readFileSync(filePath, 'utf8');
    }
    else {
      fs.writeFileSync(filePath, nextLink)
    }

  }
}
module.exports = ReadMSEmailConnector;
