package muti.kafka.custom.partitioner.user;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * UserServiceImpl.
 * 
 * Basic implementation of the UserService interface.
 * 
 * @author Andrea Muti
 * created: 26 nov 2017
 *
 */
public class UserServiceImpl implements IUserService {

	// Pairs of username and id
	private Map<String, Integer> usersMap;

	public UserServiceImpl() {
		
		usersMap = new HashMap<String, Integer>();
		usersMap.put("Tom", 1);
		usersMap.put("Mary", 2);
		usersMap.put("Alice", 3);
		usersMap.put("Daisy", 4);
		usersMap.put("Helen", 5);
	}


	@Override
	public Integer findUserId(String userName) {
		
		return usersMap.get(userName);
	}

	@Override
	public List<String> findAllUsers() {
		return new ArrayList<String>(usersMap.keySet());

	}

}