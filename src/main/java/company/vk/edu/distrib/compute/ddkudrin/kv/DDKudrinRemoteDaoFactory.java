package company.vk.edu.distrib.compute.ddkudrin.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public class DDKudrinRemoteDaoFactory implements RemoteDaoFactory<String> {

    @Override
    public Dao<String> create(int... ports) {
        if (ports.length < 1) {
            throw new IllegalArgumentException("Expected at least one KV service port");
        }
        return new DDKudrinRemoteDao(ports[0]);
    }
}
