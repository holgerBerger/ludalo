/*
 * x3 faster version of file examiner
 *  c++ -O3 -std=c++0x lustre_data_examiner.cpp
 * holger berger 2014
 */

#include <string> 
#include <iostream> 
#include <fstream>  
#include <vector>  
#include <map>  
#include <set>  
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp> 
#include <boost/lambda/lambda.hpp> 

int main(int argc, char **argv) {
	std::map<std::string, std::vector<std::string> > servers;
	int values = 0;
	int nids = 0;
	int nrsamples = 0;
	std::set<int> timestamps;
	std::set<std::string> sources;

	std::ifstream ifs (argv[1], std::ifstream::in);
	
	std::string line;
	while (std::getline(ifs, line)) {
		std::vector<std::string> sp;
		boost::split(sp, line, boost::lambda::_1 == ';');
		// boost::split(sp, line, boost::is_any_of(";"));
		if (boost::starts_with(line, "#")) {
			servers[sp[1]] = sp;
			nids = std::max(nids, (int)(sp.size()-4));
		} else {
			nrsamples++;
			timestamps.insert(boost::lexical_cast<int>(sp[1]));
			sources.insert(sp[2]);
			for (int i=4; i<sp.size(); i++)	{
				if (sp[i] != "") values++;
			}
		}
	}
	std::cout << "File contains:\n";
	std::cout << " samples from servers:";
	for(auto &i: servers) {
		std::cout << " " << i.first;
	}
	std::cout << std::endl;

	std::cout << " sources:"; for(auto &i: sources) std::cout << " " << i;
	std::cout << std::endl;
		
	std::cout << " #sources: " << sources.size() << std::endl;
	std::cout << " #samples: " << nrsamples << std::endl;
    std::cout << " #time samples: " << timestamps.size() << std::endl;
	std::cout << " #values: " << values << std::endl;
	std::cout << " #max values: " << timestamps.size()*sources.size()*nids<< std::endl;
	std::cout << " fill in: " << (double)values/(double)(timestamps.size()*sources.size()*nids)*100.0 <<"%\n";
	std::vector<int> ts(timestamps.size());
	std::copy(timestamps.begin(), timestamps.end(), ts.begin());
	time_t t = ts[0];
	std::cout << " first sample: " << std::ctime(&t);
	t = ts[ts.size()-1];
	std::cout << " last sample: " << std::ctime(&t);


}
