package company.vk.edu.distrib.compute.v11qfour;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class KVServiceFactoryImpl extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> dao = new PersistentDao();
        return new KVServiceImpl(port, dao);
    }
}
