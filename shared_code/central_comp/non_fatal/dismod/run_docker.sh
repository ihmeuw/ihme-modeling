docker build -t dismod_ode .
docker run -v strDir:strDir -v $(pwd):strDir -v $HOME:/root -it dismod_ode /bin/bash
