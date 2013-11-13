/* Formating function for row details */
function fnFormatDetails ( oTable, nTr )
{
    var aData = oTable.fnGetData( nTr );
    var sOut = 'hello world '+aData[2]+ 'hello world';
    return sOut;
}



$(document).ready(function() {
    /*
     * Insert a 'details' column to the table
     */
    var nCloneTh = document.createElement( 'th' );
    var nCloneTd = document.createElement( 'td' );
    nCloneTd.innerHTML = '<img src="/images/glyphicons_190_circle_plus.png">';
    nCloneTd.className = "center";
    
    $('#main thead tr').each( function () {
        this.insertBefore( nCloneTh, this.childNodes[0] );
    } );
    
    $('#main tbody tr').each( function () {
        this.insertBefore(  nCloneTd.cloneNode( true ), this.childNodes[0] );
    } );
    
    /*
     * Initialse DataTables, with no sorting on the 'details' column
     */
    var oTable = $('#main').dataTable( {
        "sAjaxSource": url,
        "aoColumns": column_list
        "aoColumnDefs": [
            { "bSortable": false, "aTargets": [ 0 ] }
        ],
        "aaSorting": [[1, 'asc']]
    });
    
    /* Add event listener for opening and closing details
     * Note that the indicator for showing which row is open is not controlled by DataTables,
     * rather it is done here
     */
    $('#main tbody td img').live('click', function () {
        var nTr = this.parentNode.parentNode;
        if ( this.src.match('circle_minus') )
        {
            /* This row is already open - close it */
            this.src = "/images/glyphicons_190_circle_plus.png";
            oTable.fnClose( nTr );
        }
        else
        {
            /* Open this row */
            this.src = "/images/glyphicons_191_circle_minus.png";
            oTable.fnOpen( nTr, fnFormatDetails(oTable, nTr), 'details' );
        }
    } );
} );




