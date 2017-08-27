package org.smcql.codegen.smc;

import java.io.IOException;
import org.smcql.util.Utilities;

public class ByteArrayClassLoader extends ClassLoader {

	byte[] byteArray = null;
	String className = null;
	
	public ByteArrayClassLoader(String packageName, byte[] data, ClassLoader parent) {
		super(parent);
		className = packageName + ".NoClass";
		byteArray = data;
	}


    public ByteArrayClassLoader() {
		//  Auto-generated constructor stub
	}


	public Class findClass(String packageName) {
    	
    	// load local file 
    	if(className == null && byteArray == null) {
           	try {
    			byteArray = Utilities.readGeneratedClassFile(packageName);
    			className = packageName + ".NoClass";
    			} catch (IOException e) {
    				e.printStackTrace();
    			}

    		
    	}
    	
    	return defineClass(className,byteArray,0,byteArray.length);
    }

}
