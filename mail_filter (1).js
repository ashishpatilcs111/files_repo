const modules = global.modules;
const { GenericScriptBase, util } = modules;

class MailFilter extends GenericScriptBase {
    async process() {
        let self = this;
        let { scriptParams, data } = self;
        let rslt = { rc: 0, data: {} };

        let { subject, received_date, email_message, message_uid,sender } = data;

        subject = subject || '';

        // let allowed_sender = ['vaibhav@featsytems.com','pankaj.pandey@hdfcsec.com','kavita.devadiga@hdfcsec.com','services@hdfcsec.com']
        // rslt.data.skip = (!allowed_sender.includes( sender.toLowerCase()));
        // if (rslt.data.skip)
        //     return rslt;
        // Regex patterns for reply and forward detection (case-insensitive)
        const replyPattern = /(?:re:)|(?:on\s.*wrote:)|(?:from:.*sent:)/i;
        const forwardPattern = /(?:forwarded message|fwd:|fw:)/i;

        // Function to detect email type
        function detectEmailType(subject, body) {
            if (subject.startsWith('re:')) return 'reply';
            if (subject.startsWith('fwd:') || subject.startsWith('fw:')) return 'forward';

            if (body) {
                const lowerBody = body.toLowerCase();
                if (replyPattern.test(lowerBody)) return 'reply';
                if (forwardPattern.test(lowerBody)) return 'forward';
            }

            return 'fresh';
        }

        // Detect the type
        const emailType = detectEmailType(subject.toLowerCase(), email_message);

        // Process based on email type
        if (emailType === 'fresh') {
            if (email_message) {
                console.log(`Fresh Email - Subject: ${subject}`);
            } else {
                rslt.data.skip = true;
            }
        } else {
            rslt.data.skip = true;
        }

        return rslt;
    }
}

module.exports = MailFilter;