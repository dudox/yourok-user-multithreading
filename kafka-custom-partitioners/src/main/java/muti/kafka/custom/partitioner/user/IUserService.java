package muti.kafka.custom.partitioner.user;

import java.util.List;

/**
 * IUserService.
 * 
 * Operations for handling Users.
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public interface IUserService {
	
	/**
	 * Retrieve id of the User based on the given userName parameter.
	 * 
	 * @param userName the name of the user
	 * @return the id of the user whose username is provided as input argument.
	 */
	public Integer findUserId(String userName);

	/**
	 * Return all the users in the system.
	 *
	 * @return a list of all users in the system.
	 */
	public List<String> findAllUsers();
}
