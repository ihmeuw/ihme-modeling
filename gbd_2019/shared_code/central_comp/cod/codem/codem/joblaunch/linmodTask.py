import logging
from pathlib import Path
import subprocess

import codem

logger = logging.getLogger(__name__)


def get_rscript(scriptname):
	install_dir = Path(codem.__file__).parents[0] / "linmod" / "{}.R".format(scriptname)
	return str(install_dir)


class LinMod(object):
	def __init__(self, scriptname, model_version_id, db_connection, model_dir, cores, more_args=None):
		self.scriptname = scriptname
		self.model_version_id = model_version_id
		self.db_connection = db_connection
		self.model_dir = model_dir
		self.cores = cores
		self.more_args = more_args

		logger.info("Submitting {}".format(scriptname))
		self.call = 'FILEPATH ' + \
						get_rscript(self.scriptname) + ' ' + \
						str(self.model_version_id) + ' ' + \
						self.model_dir + ' ' + \
						self.db_connection + \
						'ADDRESS' + ' ' + \
						str(self.cores)
		if self.more_args:
			for arg in self.more_args:
				self.call += ' ' + str(arg)
		logger.info(self.call)

	def run(self):
		exit_status = subprocess.call(self.call, shell=True)
		return exit_status

