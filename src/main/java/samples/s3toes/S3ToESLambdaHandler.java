package samples.s3toes;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.HttpsURLConnection;

import java.io.OutputStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;

public class S3ToESLambdaHandler implements RequestHandler<S3Event, Object> {
	
	private static final String AWS_HOST = "https://<aws-host>";
	
	private static final String AWS_URL = AWS_HOST + "/<lambda-s3-index>/<lambda-type>";
	
	private static final Pattern IP_PATTERN = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)");
	private static final Pattern TIME_PATTERN = Pattern.compile("\\[(\\d+\\/\\w\\w\\w\\/\\d\\d\\d\\d:\\d\\d:\\d\\d:\\d\\d\\s-\\d\\d\\d\\d)\\]");
	private static final Pattern MESSAGE_PATTERN = Pattern.compile("\\\"(.+)\\\"");
	

	@Override
	public Object handleRequest(S3Event event, Context context) {
		LambdaLogger logger = context.getLogger();
		
		GetSessionTokenRequest sessionTokenRequest = new GetSessionTokenRequest();
		sessionTokenRequest.setDurationSeconds(7200);
		GetSessionTokenResult sessionTokenResult = AWSSecurityTokenServiceClientBuilder.defaultClient().getSessionToken(sessionTokenRequest);
		Credentials credentials = sessionTokenResult.getCredentials();
		
		BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
				credentials.getAccessKeyId(),
				credentials.getSecretAccessKey(),
				credentials.getSessionToken());
		
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion("<region>")
				.withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
				.build();
		
		URL url;
		try {
			url = new URL(AWS_URL);
		} catch (MalformedURLException e) {
			logger.log(e.getMessage());
			return null;
		}
		
		HttpsURLConnection connection;
		try {
			connection = (HttpsURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Content-Type", "application/json); charset=UTF-8");
			connection.setRequestProperty("x-amz-security-token", credentials.getSessionToken());
			connection.setDoOutput(true);
			connection.connect();
		} catch (IOException e) {
			logger.log(e.getMessage());
			return null;
		}
		
		List<S3EventNotificationRecord> records = event.getRecords();
		
		for (S3EventNotificationRecord record : records) {
			String bucketName = record.getS3().getBucket().getName();
			String key = record.getS3().getObject().getKey();
			
			S3Object s3Object = s3Client.getObject(bucketName, key);
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
			
			String line = null;
			try {
				while ((line = reader.readLine()) != null) {
					Matcher ipMatcher = IP_PATTERN.matcher(line);
					Matcher timeMatcher = TIME_PATTERN.matcher(line);
					Matcher messageMatcher = MESSAGE_PATTERN.matcher(line);
					
					ipMatcher.find();
					timeMatcher.find();
					messageMatcher.find();
					
					String ip = ipMatcher.group(1);
					String time = timeMatcher.group(1);
					String message = messageMatcher.group(1);
					
					byte[] json = (ip + time + message).getBytes(StandardCharsets.UTF_8);
					
					try (OutputStream outputStream = connection.getOutputStream()) {
						outputStream.write(json);
					}
				}
			} catch (IOException e) {
				logger.log(e.getMessage());
				return null;
			} finally {
				try {
					s3Object.close();
				} catch (IOException e) {}
			}
		}
		
		return null;
	}

}
