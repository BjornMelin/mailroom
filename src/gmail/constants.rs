pub(super) const TOKEN_REFRESH_LEEWAY_SECS: u64 = 60;
pub(super) const GMAIL_MAX_RETRY_ATTEMPTS: usize = 4;
pub(super) const GMAIL_INITIAL_RETRY_DELAY_MS: u64 = 1_000;
pub(super) const MESSAGE_CATALOG_FULL_FIELDS: &str =
    "id,threadId,labelIds,snippet,historyId,internalDate,sizeEstimate,payload";
pub(super) const MESSAGE_CATALOG_FIELDS: &str = concat!(
    "id,threadId,labelIds,snippet,historyId,internalDate,sizeEstimate,",
    "payload(",
    "headers(name,value),",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(partId,mimeType,filename,headers(name,value),body(attachmentId,size),parts(partId))",
    ")",
    ")",
    ")",
    ")",
    ")"
);
