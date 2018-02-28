
DEF_MAX_TIME = 104 # weeks for 2 years
MIN_TIME = -1  # weeks 

def plotdata_with_errors (plt, x, y, yerr, maxx=DEF_MAX_TIME, minx=MIN_TIME):
    
    #plot data and errors
    plt.plot(x, y, "ko", label='Data')
    plt.errorbar(x, y, fmt='ko', yerr=y_err)
    
    plt.plot([minx,maxx], [0,0], 'k--') # for showing 0
    plt.xlim(minx, maxx)
    
    plt.xlabel("Weeks Since Vulnerability Release")
    plt.ylabel("% Patched")
    plt.ylim(-15,125.)

def plotdata_with_errors_and_func (plt, x, y, yerr, p, perr, fitfunc, maxx=DEF_MAX_TIME, minx=MIN_TIME):
    
    plotdata_with_errors(plt, x, y, yerr, maxx, minx)
    
    # plot with 2 sigma bounds
    perr[0] = perr[0] * 2.
    perr[1] = perr[1] * 2.
    
    #plot function w/ bounds
    plt.plot(x, fitfunc(p, x), 'r-', label='Fitted Func')
    pmin = (p[0] - perr[0], p[1] - perr[1])
    pmax = (p[0] + perr[0], p[1] + perr[1])
    plt.plot(x, fitfunc(pmin, x), 'b--', label='Lower bound')
    plt.plot(x, fitfunc(pmax, x), 'b--', label='Upper bound')
    plt.plot([-25,700], [0,0], 'k--') # for showing 0
        
def plotdata (plt, center, x, y, yerr, p, perr, redchisq, fitfunc, maxx=DEF_MAX_TIME, minx=MIN_TIME):

    plotdata_with_errors_and_func(plt, x, y, yerr, p, perr, fitfunc, maxx, minx)
    
    mp = midpoint(p, perr)
    plt.legend()
    max_index=len(x)-1
    max_x=x[max_index]
    if center != None:
        plt.title(f" {center}\n %50 patched by {mp}\n redchisq={redchisq}\n {max_index}")

def plotdata_and_residuals(plt, center, x, y, yerr, p, perr, redchisq, fitfunc, maxx=DEF_MAX_TIME, minx=MIN_TIME):
    
    fig = plt.figure(1)
    
    frame1=fig.add_axes((.1,.3,.8,.6))
    plotdata_with_errors_and_func(plt, x, y, yerr, p, perr, fitfunc, maxx, minx)
    plt.title(f" {center}")
    #plt.grid()
    
    # fit and plot residuals w/ a constant
    difference = fitfunc(pfit, x) - y
    
    # try to fit with constant line
    pfit_cnst, perr_cnst, redchisq_cnst = fit_leastsq([0], x, difference, fitfunc_cnst, y_err)
    
    frame2=fig.add_axes((.1,.1,.8,.2))
    #plotdata_with_errors_and_func(plt, x, difference, yerr, pfit_cnst, perr_cnst, fitfunc_cnst)
    plotdata_with_errors(plt, x, difference, yerr)
    plt.ylabel("Residuals")
    #plt.plot(x, fitfunc_cnst(pfit_cnst, x), 'r-', label='Fitted Const')
    #plt.title(f" Fitted value {pfit_cnst[0]} +/- {perr_cnst[0]}\n redchi:{redchisq_cnst}")
    plt.ylim(-125.,125.)
    
