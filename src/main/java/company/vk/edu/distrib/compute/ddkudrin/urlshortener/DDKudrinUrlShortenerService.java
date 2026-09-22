package company.vk.edu.distrib.compute.ddkudrin.urlshortener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;

import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;

public class DDKudrinUrlShortenerService implements UrlShortenerService {

    private final HttpServer server;
    private final Dao<String> linkDao;
    private final Dao<String> userDao;

    public DDKudrinUrlShortenerService(int port) throws IOException {
        this.linkDao = new DDKudrinPersistentDao(
                Path.of(System.getProperty("user.home"), "data", "links.wal")
        );
        this.userDao = new DDKudrinPersistentDao(
                Path.of(System.getProperty("user.home"), "data", "users.wal")
        );
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", new DDKudrinStatusHandler());
        this.server.createContext(
            "/v0/links",
            new DDKudrinAuthMiddleware(new DDKudrinLinksHandler(linkDao), userDao)
        );
        this.server.createContext("/internal/users", new DDKudrinUserHandler(userDao));
        this.server.createContext("/", new DDKudrinRedirectHandler(linkDao));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override 
    public void stop() {
        server.stop(1);
    }
}
