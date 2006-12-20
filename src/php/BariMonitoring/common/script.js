var old,theTop,menu,temp,c;

function scale()
{
        if (temp){ clearTimeout(temp)};
        if (!document.all && !document.getElementById)
        {
                alert('The script on this page does\'t work in your browser');
                return;
        }
        var extra = 0; // default NN6 & IE5Mac, not perfect IE4Mac, Too Bad
        if (document.all && navigator.userAgent.indexOf('Win') != -1){
                extra = 0; //default IEWin
        }
        // extra += 2 per point of cellpadding; not necessary NN6.01
        if (!document.all || navigator.userAgent.indexOf('Mac') == -1){
                extra +=2;
        }
        var a = new getObj('fixedtable');
        var b = new getObj('normaltable');
 
        for (i=0;i<stuff.length;i++)
        {
        if (document.getElementById(stuff[i]+'head')){
                y = new getObj(stuff[i]+'body');
                z = new getObj(stuff[i]+'head');
                x = y.obj.offsetWidth - extra;
//		 x = y.obj.offsetWidth - 1;
                z.style.width = x + 'px';
        }
        }

        theTop = 0;

        // calc top before moving the relative table for IE5Mac

        a.style.top = theTop + 'px';
        old=0;
        c = b.obj.offsetTop - a.obj.offsetHeight;

        menu = new getObj('fixedtable');
        movemenu();
}

function getObj(name)
{
  if (document.getElementById)
  {
        this.obj = document.getElementById(name);
        this.style = document.getElementById(name).style;
  }
  else if (document.all)
  {
        this.obj = document.all[name];
        this.style = document.all[name].style;
  }
  else if (document.layers)
  {
        this.obj = document.layers[name];
        this.style = document.layers[name];
  }
}


function movemenu()
{
        if (window.innerHeight)
        {
                  pos = window.pageYOffset - c;
			//alert(pos);
		  //fadeIn('fixedtable',0);
                          
        }
        else if (document.documentElement && document.documentElement.scrollTop)
        {
                pos = document.documentElement.scrollTop - c - 15;

        }
        else if (document.body)
        {
                  pos = document.body.scrollTop - c;
        }
        if (pos < theTop)
        { 
                pos = theTop;

        }
        if (pos == old)
        {
                menu.style.top = pos + 'px';

        }
        old = pos;
        temp = setTimeout('movemenu()',0);
}


var IE = document.all?true:false;
if (!IE) document.captureEvents(Event.MOUSEMOVE);
tempX = 0;
tempY = 0;
document.onmousemove = getMouseXY;

function getMouseXY(e) {
//alert(e);
if (IE) {
tempX = event.clientX + document.body.scrollLeft;
tempY = event.clientY + document.body.scrollTop;
} else {
tempX = e.pageX;
tempY = e.pageY;
}
if (tempX < 0){tempX = 0}
if (tempY < 0){tempY = 0}
}
function vpopup(e){
document.getElementById(e).style.top=-50+tempY;
document.getElementById(e).style.left=tempX+10;
return true;
}

/******** start opacita' (two function)********/
function fadeIn(objId,opacity) {
  if (document.getElementById) {
    obj = document.getElementById(objId);
    if (opacity <= 100) {
      setOpacity(obj, opacity);
      opacity += 10;
      window.setTimeout("fadeIn('"+objId+"',"+opacity+")", 100);
    }
  }
}

function setOpacity(obj, opacity) {
  opacity = (opacity == 100)?99.999:opacity;
  
  // IE/Win
  obj.style.filter = "alpha(opacity:"+opacity+")";
  
  // Safari<1.2, Konqueror
  obj.style.KHTMLOpacity = opacity/100;
  
  // Older Mozilla and Firefox
  obj.style.MozOpacity = opacity/100;
  
  // Safari 1.2, newer Firefox and Mozilla, CSS3
  obj.style.opacity = opacity/100;
}

/******** fine opacita'  *********/
