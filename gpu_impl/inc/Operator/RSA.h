#ifndef GPUIMPL_RSA_H
#define GPUIMPL_RSA_H


bool addbignum(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned int n);
bool subbignum(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned int n);
bool modbignum(unsigned long res[],unsigned long op1[], unsigned long op2[],unsigned int n);
bool modnum(unsigned long res[],unsigned long op1[], unsigned long op2[],unsigned int n);
bool modmult1024(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned long mod[]);
bool rsa1024(unsigned long res[], unsigned long data[], unsigned long expo[],unsigned long key[]);
bool multbignum(unsigned long res[], unsigned long op1[], unsigned int op2 ,unsigned int n);
unsigned int bit_length(unsigned long op[],unsigned int n);
int compare(unsigned long op1[], unsigned long op2[],unsigned int n);
bool slnbignum(unsigned long res[], unsigned long op[],unsigned int len, unsigned int n);//shift left by n
bool srnbignum(unsigned long res[], unsigned long op[],unsigned int len, unsigned int n);


bool rsa1024(unsigned long res[], unsigned long data[], unsigned long expo[],unsigned long key[])
{
    int i,j,expo_len;
    unsigned long mod_data[18]={0},result[18]={0};
    unsigned long temp_expo=0;

    modbignum(mod_data,data,key,16);
    result[0] = 1;
    expo_len = bit_length(expo,16) /64;
    for(i=0;i<expo_len+1;i++)
    {
        temp_expo = expo[i];
        for(j=0;j<64;j++)
        {
            if(temp_expo & 0x1UL)
                modmult1024(result,result,mod_data,key);

            modmult1024(mod_data,mod_data,mod_data,key);
            temp_expo = temp_expo >> 1;
        }
    }
    for(i=0;i<16;i++)
        res[i]=result[i];


    return 1;
}




bool addbignum(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned int n)
{
    unsigned int i;
    unsigned long j,k,carry=0;
    for(i = 0; i<n; i++)
    {
        j = (op1[i] & 0xffffffff) + (op2[i] & 0xffffffff) + carry;

        k = ((op1[i]>>32) & 0xffffffff) + ((op2[i]>>32) & 0xffffffff) + ((j>>32) & 0xffffffff);

        carry = ((k>>32) & 0xffffffff);

        res[i] = ((k & 0xffffffff)<<32)  | (j & 0xffffffff);
    }
    res[i] = carry;
    return 0;
}



bool multbignum(unsigned long res[], unsigned long op1[], unsigned int op2 ,unsigned int n)
{
    unsigned int i;
    unsigned long j,k,carry1=0,carry2=0;
    for(i = 0; i<n; i++)
    {
        j = (op1[i] & 0xffffffff) * (op2 & 0xffffffff);

        k = ((op1[i]>>32) & 0xffffffff) * (op2 & 0xffffffff);
        carry1 = ((k>>32) & 0xffffffff);
        k = (k & 0xffffffff) + ((j>>32) & 0xffffffff);
        j = (j & 0xffffffff) + carry2;
        k = k + ((j>>32) & 0xffffffff);
        carry2 = carry1 + ((k>>32) & 0xffffffff);

        res[i] = ((k & 0xffffffff)<<32)  | (j & 0xffffffff);
    }
    res[i] = carry2;
    return 0;
}
bool modmult1024(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned long mod[]) //optimized
{
    int i,j;
    unsigned long mult1[33]={0},mult2[33]={0},
            result[33]={0},xmod[33]={0};

    for(i=0;i<16;i++)
        xmod[i]=mod[i];

    for(i=0;i<16;i++)
    {
        for(j=0;j<33;j++)
        {
            mult1[j]=0;
            mult2[j]=0;
        }
        multbignum(mult1,op1,(op2[i]&0xffffffff),16);
        multbignum(mult2,op1,((op2[i]>>32)&0xffffffff),16);
        slnbignum(mult2,mult2,33,32);
        addbignum(mult2,mult2,mult1,32);

        slnbignum(mult2,mult2,33,64*i);

        addbignum(result,result,mult2,32);

    }
    modbignum(result,result,xmod,33);
    for(i=0;i<16;i++)
        res[i]=result[i];

    return 0;
}


bool modbignum(unsigned long res[],unsigned long op1[], unsigned long op2[],unsigned int n)//optimized
{
    unsigned int i;
    int len_op1,len_op2,len_dif;

    len_op1 = bit_length(op1,n);
    len_op2 = bit_length(op2,n);
    len_dif = len_op1 - len_op2;



    for(i=0;i<n;i++)
        res[i]=op1[i];

    if(len_dif < 0)
    {
        return 1;
    }

    if(len_dif == 0)
    {
        /*
        while(compare(res,op2,n)>=0)
        {
            subbignum(res,res,op2,n);
        }
         */
        return 1;
    }

    slnbignum(op2,op2,n,len_dif);
    for(i=0;i<len_dif;i++)
    {
        srnbignum(op2,op2,n,1);
        /*
        while(compare(res,op2,n)>=0)
        {
            subbignum(res,res,op2,n);
        }
         */
    }

    return 1;
}

/****************************************************************
 * bool modnum(unsigned long res[],unsigned long op1[], unsigned long op2[],unsigned int n)
 * res = op1 % op2
 * n is bit length/64
 * res must have extra 64 bits to avoid errors
 ****************************************************************/
bool modnum(unsigned long res[],unsigned long op1[], unsigned long op2[],unsigned int n)
{
    unsigned int i;
    bool result=0;
    for(i=0;i<n;i++)
        res[i]=op1[i];

    while(!result)
    {
        result = subbignum(res,res,op2,n);
    }

    addbignum(res,res,op2,n);
    res[n]=0;

    return 0;
}
/****************************************************************
* int compare(unsigned long op1[], unsigned long op2[],unsigned int n)
* returns 1 if op1>op2
* 		 -1 if op1<op2
* 		  0 if op1=op2
*****************************************************************/
int compare(unsigned long op1[], unsigned long op2[],unsigned int n)
{
    for( ; n>0; n--)
    {
        if(op1[n-1]>op2[n-1])
        {
            return 1;
        }
        else if(op1[n-1]<op2[n-1])
        {
            return -1;
        }
    }

    return 0;
}

/****************************************************************
 * bool subbignum(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned int n)
 * subtracts op2 from op1
 * returns 0 if op1>=op2
 * 		   1 if op1<op2
 * result is not valid if return value is 1 (or is in 2's compliment :P)
 * **************************************************************/
bool subbignum(unsigned long res[], unsigned long op1[], unsigned long op2[],unsigned int n)
{
    bool carry=0;
    unsigned int i;
    for(i=0;i<n;i++)
    {
        if(carry)
        {
            if(op1[i]!=0)
                carry=0;
            op1[i]--;
        }
        if(op1[i]<op2[i])
            carry = 1;

        res[i]= op1[i] - op2[i];
    }
    return carry;
}
bool slnbignum(unsigned long res[], unsigned long op[],unsigned int len, unsigned int n)//shift left by n
{
    unsigned int i,x,y;
    unsigned long j,k,carry = 0;
    x = n / 64;
    y = n % 64;

    for(i=len; i - x >0; i--)
    {
        res[i-1] = op[i - 1 - x];
    }
    for(;i>0;i--)
    {
        res[i-1] = 0;
    }
    for(i=0;i<len;i++)
    {
        j = res[i];
        k=0;
        for(x=0;x<y;x++)
        {
            if(j & 0x8000000000000000)
            {
                k = (k<<1) | 1;
            }
            else
            {
                k = (k<<1);
            }
            j = j <<1;
        }
        res[i] = j | carry;
        carry = k;
    }
    return 1;
}
bool srnbignum(unsigned long res[], unsigned long op[],unsigned int len, unsigned int n)//shift right by n
{
    unsigned int i,x,y;
    unsigned long j,k,carry = 0;
    x = n / 64;
    y = n % 64;

    for(i=0; i + x < len; i++)
    {
        res[i] = op[i + x];
    }
    for(;i<len;i++)
    {
        res[i] = 0;
    }
    for(i=len;i>0;i--)
    {
        j = res[i-1];
        k=0;
        for(x=0;x<y;x++)
        {
            if(j & 0x0000000000000001)
            {
                k = (k>>1) | 0x8000000000000000;
            }
            else
            {
                k = (k>>1);
            }
            j = j >>1;
        }
        res[i-1] = j | carry;
        carry = k;
    }
    return 1;

}
/****************************************************************
 * unsigned int bit_length(unsigned long op[],unsigned int n)
 * returns position of MSB present
 *
 *
 ****************************************************************/
unsigned int bit_length(unsigned long op[],unsigned int n)
{
    unsigned int len=0;
    unsigned int i;
    unsigned long unit = 1;
    for( ;n>0;n--)
    {
        if(op[n-1]==0)
            continue;
        for(i=64;i>0;i--)
        {
            if(op[n-1] & (unit<<(i-1)))
            {
                len = (64*(n-1)) + i;
                break;
            }

        }
        if(len)
            break;
    }
    return len;
}
/*
bool setbitbignum(unsigned long op[],unsigned int n,unsigned int bit)//sets n'th bit
{
    unsigned int q,r;
    unsigned long unit=0x1;
    q = bit / 64;
    r = bit % 64;
    if(q>=n)
        return 0;
    op[q] |= (unit<<r);
    return 1;
}
*/
#endif
