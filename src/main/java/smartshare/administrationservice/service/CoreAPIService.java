package smartshare.administrationservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import smartshare.administrationservice.configuration.CoreServerConfiguration;
import smartshare.administrationservice.dto.UploadObject;

@Slf4j
@Service
public class CoreAPIService {

    private final RestTemplate restTemplate;
    private final UriComponentsBuilder coreServerUrl;

    @Autowired
    public CoreAPIService(RestTemplate restTemplate,
                          CoreServerConfiguration coreServerConfiguration) {
        this.restTemplate = restTemplate;
        this.coreServerUrl = UriComponentsBuilder.newInstance().scheme( "http" )
                .host( coreServerConfiguration.getHostName() )
                .port( coreServerConfiguration.getPort() );
    }

    public Boolean createEmptyFolder(UploadObject uploadObject) {
        log.info( "Inside createEmptyFolder" );
        try {
            UriComponents url = coreServerUrl.replacePath( "folder/empty" ).build();
            return restTemplate.postForObject( url.toUriString(), uploadObject, Boolean.class );
        } catch (Exception e) {
            log.error( "Exception while creating folder " + e.getMessage() );
        }
        return false;
    }

    public Boolean deleteObject(String objectName, String bucketName, int ownerId) {
        log.info( "Inside deleteFolder" );
        try {
            UriComponents url = coreServerUrl.replacePath( "file" )
                    .replaceQueryParam( "objectName", objectName )
                    .replaceQueryParam( "bucketName", bucketName )
                    .replaceQueryParam( "ownerId", ownerId )
                    .build();
            restTemplate.delete( url.toUriString() );
            return true;
        } catch (Exception e) {
            log.error( "Exception while creating folder " + e.getMessage() );
        }
        return false;
    }
}
