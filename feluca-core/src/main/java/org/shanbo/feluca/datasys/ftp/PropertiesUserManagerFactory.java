package org.shanbo.feluca.datasys.ftp;

import java.io.InputStream;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.PasswordEncryptor;
import org.apache.ftpserver.usermanager.UserManagerFactory;

/**
 * add a InputStream user properties entry
 * @author lgn
 *
 */
public class PropertiesUserManagerFactory implements UserManagerFactory {

    private String adminName = "admin";

    private InputStream userDataIn;


    private PasswordEncryptor passwordEncryptor = new Md5PasswordEncryptor();

    /**
     * Creates a {@link PropertiesUserManager} instance based on the provided configuration
     */
    public UserManager createUserManager() {
          return new PropertiesUserManager(passwordEncryptor, userDataIn, adminName);   
    }

    /**
     * Get the admin name.
     * @return The admin user name
     */
    public String getAdminName() {
        return adminName;
    }

    /**
     * Set the name to use as the administrator of the server. The default value
     * is "admin".
     * 
     * @param adminName
     *            The administrator user name
     */
    public void setAdminName(String adminName) {
        this.adminName = adminName;
    }


    /**
     * Set the file used to store and read users. 
     * 
     * @param propFile
     *            A file containing users
     */
    public void setInputSteam(InputStream in) {
        this.userDataIn = in;
    }

    
    /**
     * Retrieve the password encryptor used by user managers created by this factory
     * @return The password encryptor. Default to {@link Md5PasswordEncryptor}
     *  if no other has been provided
     */
    public PasswordEncryptor getPasswordEncryptor() {
        return passwordEncryptor;
    }

    /**
     * Set the password encryptor to use by user managers created by this factory
     * @param passwordEncryptor The password encryptor
     */
    public void setPasswordEncryptor(PasswordEncryptor passwordEncryptor) {
        this.passwordEncryptor = passwordEncryptor;
    }
}
