
// Description: install the 3 programs needed to create PDFs (pdfstart, pdfappend, pdffinish)
// Version: 1.1
*/

if c(os) == "Windows" {
	quietly do "J:\Usable\Tools\ADO\pdfstart.ado"
	quietly do "J:\Usable\Tools\ADO\pdfappend.ado"
	quietly do "J:\Usable\Tools\ADO\pdffinish.ado"
}
else if c(os) == "Unix" {
	quietly do "/home/j/Usable/Tools/ADO/pdfstart.ado"
	quietly do "/home/j/Usable/Tools/ADO/pdfappend.ado"
	quietly do "/home/j/Usable/Tools/ADO/pdffinish.ado"
}
