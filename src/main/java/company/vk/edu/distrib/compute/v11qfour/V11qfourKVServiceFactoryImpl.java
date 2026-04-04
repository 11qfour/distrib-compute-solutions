package company.vk.edu.distrib.compute.v11qfour;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class V11qfourKVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> dao = new V11qfourPersistentDao();
        return new V11qfourKVServiceFactory(port, dao);
    }
}
