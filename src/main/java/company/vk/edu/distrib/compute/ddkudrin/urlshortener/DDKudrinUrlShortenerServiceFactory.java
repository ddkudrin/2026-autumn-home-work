package company.vk.edu.distrib.compute.ddkudrin.urlshortener;

import java.io.IOException;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

public class DDKudrinUrlShortenerServiceFactory extends AbstractHttpServiceFactory<DDKudrinUrlShortenerService> {

    @Override
    protected DDKudrinUrlShortenerService doCreate(int port) throws IOException {
        return new DDKudrinUrlShortenerService(port);
    }
}
