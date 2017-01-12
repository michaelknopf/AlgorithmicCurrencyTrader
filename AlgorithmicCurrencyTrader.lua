Parameters = {}
Parameters.ATRperiods = 30

Parameters.levelATRSeparation = .1
Parameters.stopATRDistance = 1.8
Parameters.levelAggression = .01
Parameters.stopMoveATRDistance = 1.4
Parameters.entryLevelsBack = 1
Parameters.stopMoveMinLevelsBack = 2

Parameters.lotSize = 1000
Parameters.riskAmount = 100


----------------
-- Line Class --
----------------

-- Represents a line in slope-intercept form.  All x-values
-- are represented as periods corresponding to the parent
-- LineManager's source (price stream).

-- Constructor Parameters:
-- ...self-explanatory...
-- r: The correlation coefficient of the fit used to create the line.

Line = {}
Line.__index = Line

function Line.new(from, to, slope, int, r)
    self = setmetatable({}, Line)
    self.from = from
    self.to = to
    self.slope = slope
    self.int = int
    self.r = r
    self.artistID = nil
    return self
end

function Line.modelRange(from, to, source)
    -- Performs a linear fit of the data in source,
    -- beginning at from and ending at to (both inclusive).
    local y = {}
    for i=from,to do
        y[#y+1] = source[i]
    end
    return Line.fit(from, y)
end

function Line.fit(from, y)
    -- Performs a linear fit of the data in the vector y.
    -- The vector of x-values is assumed to begin at 0 and
    -- end at #y - 1.  The fit does contains an intercept.
    Timer.start("Fit")
    local n = #y - 1
    local x2 = n*(n+1)*(n+2)/12
    local y2 = 0
    local xy = 0
    local xBar = n/2
    local yBar = 0;
    
    for i=1,n+1 do
        yBar = yBar + y[i]
    end
    yBar = yBar / (n+1)
    
    for i=1,(n+1) do
        yi = y[i] - yBar
        y2 = y2 + yi^2
        xy = xy + (i - 1 - xBar)*yi
    end
    
    local slope = xy / x2;
    local int = yBar - slope*xBar
    if x2*y2 == 0 then
        r = 1
    else
        r = xy / math.sqrt(x2*y2)
    end
    
    Timer.stop("Fit")
    
    return Line.new(from, from + #y - 1, slope, int, math.abs(r))
end

function Line.__call(self,x)
    -- Get function value at x.
    return self.slope*(x - self.from) + self.int
end

Line.__tostring = function(line)
    return "from: " .. tostring(line.from) .. ",   " ..
           "to: " .. tostring(line.to) .. ",   " ..
           "slope: " .. tostring(line.slope) .. ",   " ..
           "int: " .. tostring(line.int) .. ",   " ..
           "r: " .. tostring(line.r)
end

-----------------------
-- LineManager Class --
-----------------------

-- This object contains a list of line objects and manages their initialization and updating.

-- Constructor Parameters:
-- source: The tick stream of price data.
-- high:   Whether the price at a given period is the maximumum or minimum price achieved during that period.
-- periodsBack: How many periods back from the current period to initialize the model.
-- minLinePoints: The minimum number of periods a line must contain.  If this is <= 2, then lines will be allowed
--                to be perfect fits.
-- maxLinePoints: The max number of points on a line.
-- searchDepth: How many periods forward or backward to search looking for the best endpoint of a line.
-- smoothings: How many times to perform smoothing operation during updates.
-- updateDepth: How far back from the current period to perform smoothings during updates.

LineManager = {}
LineManager.__index = LineManager

function LineManager.new(source, high, periodsBack, minLinePoints, maxLinePoints, searchDepth, smoothings, updateDepth)
    local self = setmetatable({}, LineManager)
    self.lines = {}
    self.source = source
    self.high = high
    self.initialized = false
    self.periodsBack = periodsBack or 200
    self.minLinePoints = minLinePoints or 3
    self.maxLinePoints = maxLinePoints or 25
    self.searchDepth = searchDepth or 25
    self.smoothings = smoothings or 10
    self.updateDepth = updateDepth or 10
    return self
end

function LineManager:initialize()
    self:insert(self:searchForward(self.startPeriod))
    self:completeLines()
    self:smooth()
    self.lastUpdatePeriod = self.source:size() - 1
    self.initialized = true
end

function LineManager:update()
    if self.lastUpdatePeriod < self.source:size() - 1 then
        Timer.start("Complete Lines")
        self:completeLines()
        Timer.stop("Complete Lines")
        Timer.start("Smoothings")
        self:smooth(self.updateDepth)
        Timer.stop("Smoothings")
        self.lastUpdatePeriod = self.source:size() - 1
    end
end

function LineManager:setStart(startPeriod, startDate)
    self.startPeriod = startPeriod or self.source:size() - 1 - self.periodsBack
    self.startDate = startDate or self.source:date(self.startPeriod)
end

function LineManager:insert(line, k)
    local k = k or #self.lines + 1
    table.insert(self.lines, k, line)
    Artist.drawLine(self.lines[k])
end

function LineManager:remove(k)
    Artist.removeLine(self.lines[k].artistID)
    return table.remove(self.lines, k)
end

function LineManager:replace(line, k)
    Artist.removeLine(self.lines[k].artistID)
    self.lines[k] = line
    Artist.drawLine(self.lines[k])
end

function LineManager:searchForward(from)
    local minTo = math.min(from + self.minLinePoints - 1, self.source:size() - 1)
    local maxTo = math.min(from + self.maxLinePoints - 1, self.source:size() - 1)
    if minTo >= maxTo then
        return nil;
    end
    
    local bestLine = Line.new(0,0,0,0,0)
    for to = minTo, math.min(minTo + self.searchDepth, maxTo) do
        line = Line.modelRange(from, to, self.source)
        if line.r >= bestLine.r then bestLine = line end
    end
    
    return bestLine
end

function LineManager:searchBackward(to)
    local minFrom = math.max(to - (self.maxLinePoints - 1), self.source:first())
    local maxFrom = math.max(to - (self.minLinePoints - 1), self.source:first())
    if minFrom >= maxFrom then
        return nil;
    end
    
    local bestLine = Line.new(0,0,0,0,0)    
    for from = minFrom, math.min(minFrom + self.searchDepth, maxFrom) do
        line = Line.modelRange(from, to, self.source)
        if line.r >= bestLine.r then bestLine = line end
    end
    
    return bestLine
end

function LineManager:completeLines()
    local line = self:searchForward(self.lines[#self.lines].to)
    while line ~= nil do
        line = self:searchForward(self.lines[#self.lines].to)
        self:insert(line)
    end
end

function LineManager:modelPiecewise(from, to, valuation)
    -- Models the range as a piecewise line consisting of two pieces.
    -- Searches for the best place to place the corner where the two lines meet.
    -- valuation: The function used to evaluate the goodness of fit.
    local minMid = from + (self.minLinePoints -1)
    local maxMid = to - (self.minLinePoints - 1)
    
    local bestLeft
    local bestRight
    local val = 0
    
    for mid = minMid, maxMid do
        left = Line.modelRange(from, mid, self.source)
        right = Line.modelRange(mid, to, self.source)
        if valuation(left,right) > val then
            bestLeft = left
            bestRight = right
            val = valuation(bestLeft, bestRight)
        end
    end
    
    return bestLeft, bestRight
end

function LineManager:adjustCorner(k, valuation)
    
    local valuation = valuation or function(line1, line2)
        return line1.r^2 + line2.r^2
    end
    
    if k == 1 then
        self:replace(self:searchBackward(self.lines[1].to), 1)
    elseif k == #self.lines + 1 then
        self:replace(self:searchForward(self.lines[#self.lines].from), #self.lines)
    else
        assert(k > 1 and k <= #self.lines, "Corner index out of range")
        left, right = self:modelPiecewise(self.lines[k-1].from, self.lines[k].to, valuation)
        self:replace(left, k-1)
        self:replace(right, k)
    end
end

function LineManager:splitLine(k, alpha, valuation)

    local alpha = alpha or 2
    local valuation = valuation or function(line1, line2)
        return ((line1.r^2 + line2.r^2)/2)^(1/2)
    end
    local left, right = self:modelPiecewise(self.lines[k].from, self.lines[k].to, valuation)
    if left ~= nil and right ~= nil and valuation(left, right) >= valuation(self.lines[k], self.lines[k])^alpha then
        self:replace(right, k)
        self:insert(left, k)
        return 0
    end
    return 1
end

function LineManager:smooth(depth)
    local minperiod, k
    if depth == nil then
        minPeriod = 0
    else
        minPeriod = self.source:size() - 1 - depth
    end
    
    for i = 1, self.smoothings do
        k = #self.lines + 1
        while k >= 2 and self.lines[k-1].from > minPeriod do
            self:adjustCorner(k)
            k = k - 1
        end
        k = #self.lines
        while k >= 1 and self.lines[k].from > minPeriod do
            self:splitLine(k)
            k = k - 1
        end
        self:completeLines()
    end
end

-----------------
-- Level Class --
-----------------

Level = {}
Level.__index = Level

function Level.new(period, price, high, authentic, strength, leftSlope, rightSlope)
    local self = setmetatable({}, Level)
    self.price = price
    self.period = period
    self.strength = strength
    self.high = high
    self.authentic = authentic
    self.leftSlope = leftSlope
    self.rightSlope = rightSlope
    return self
end

function Level:setAuthentic(authentic)
    if self.strength == 0 then
        self.authentic = false
    else
        self.authentic = authentic
    end
    Artist.removeLine(self.artistID)
    Artist.drawLevel(self)
end
------------------------
-- LevelManager Class --
------------------------

LevelManager = {}
LevelManager.__index = LevelManager

function LevelManager.new(source, high, periodsBack, minLinePoints, maxLinePoints, searchDepth, smoothings, updateDepth, levelAuthenticityThreshold, atr)
    local self = setmetatable({}, LevelManager)
    if high then
        self.source = source.high
    else
        self.source = source.low
    end
    self.high = high
    self.periodsBack = periodsBack
    self.minLinePoints = minLinePoints
    self.maxLinePoints = maxLinePoints
    self.searchDepth = searchDepth
    self.smoothings = smoothings
    self.updateDepth = updateDepth
    self.levelAuthenticityThreshold = levelAuthenticityThreshold
    self.lineManager = LineManager.new(self.source, high, periodsBack, minLinePoints, maxLinePoints, searchDepth, smoothings, updateDepth)
    self.levels = {}
    self.periodsBack = periodsBack or 200
    self.rightExtremes = {}
    self.ATR = atr
    self.initialized = false
    return self
end

function LevelManager:initialize()
    self:updateRightExtremes()
    if self.lineManager.initialized == false then
        self.lineManager:initialize()
    end
    self:updateLevels()
    self.initialized = true
end

function LevelManager:update()
    Timer.start("Update Right Extremes")
    self:updateRightExtremes()
    Timer.stop("Update Right Extremes")
    self.lineManager:update()
    Timer.start("Update Levels")
    self:updateLevels(self.updateDepth)
    Timer.stop("Update Levels")
end

function LevelManager:setStart(startPeriod, startDate)
    self.startPeriod = startPeriod or self.source:size() - 1 - self.periodsBack
    self.startDate = startDate or self.source:date(self.startPeriod)
    self.lineManager:setStart(self.startPeriod, self.startDate)
end


function LevelManager:insert(level, k)
    table.insert(self.levels, level)
    Artist.drawLevel(self.levels[#self.levels])
end

function LevelManager:replace(level, k)
    if self.levels[k] ~= nil and self.levels[k].artistID ~= nil then
        Artist.removeLine(self.levels[k].artistID)
    end
    self.levels[k] = level
    Artist.drawLevel(self.levels[k])
end

function LevelManager:remove(k)
    Artist.removeLine(self.levels[k].artistID)
    return table.remove(self.levels, k)
end

function LevelManager:getLevel(k)
    local left = self.lineManager.lines[k]
    local right = self.lineManager.lines[k + 1]
    
    if right == nil then
        return nil
    end
    
    local period = left.to
    local price = (left(period) + right(period)) / 2
    local strength
    
    if (self.high and left.slope > 0 and right.slope < 0) or (not self.high and left.slope < 0 and right.slope > 0) then
        strength = 1
    else
        strength = 0
    end
    
    local authentic = strength ~= 0 and (period + self.levelAuthenticityThreshold.x > self.source:size() - 2 or
        (self.high and price >= self.rightExtremes[period + self.levelAuthenticityThreshold.x] - self.levelAuthenticityThreshold.y * self.ATR(period)
        or not self.high and price <= self.rightExtremes[period + self.levelAuthenticityThreshold.x] + self.levelAuthenticityThreshold.y * self.ATR(period)))

    return Level.new(period, price, self.high, authentic, strength, left.slope, right.slope)
end
function LevelManager:updateRightExtremes()
    local lastPeriod = self.source:size() - 1
    local f
    
    self.rightExtremes[lastPeriod - 1] = self.source[lastPeriod]
    if self.high then
        f = "max"
    else
        f = "min"
    end
    local k = lastPeriod - 2
    while k >= math.max(0, self.startPeriod - self.periodsBack) do
        extreme = math[f](self.source[k+1], self.rightExtremes[k + 1])
        self.rightExtremes[k] = extreme
        k = k - 1
        if self.initialized and extreme == self.source[k+1] then
            return
        end
    end
end

function LevelManager:updateLevels(depth)
    local minPeriod
    if depth == nil then
        minPeriod = 0
    else
        minPeriod = self.source:size() - 1 - depth
    end
    
    local k = #self.lineManager.lines - 1
    while k >= 2 and self.lineManager.lines[k].from >= minPeriod do
        self:replace(self:getLevel(k), k)
        k = k - 1
    end
    for _, level in pairs(self.levels) do
        if level.authentic and level.period + self.levelAuthenticityThreshold.x <= self.source:size() - 2 and
                ((self.high and level.price < self.rightExtremes[level.period] - self.levelAuthenticityThreshold.y * self.ATR(period)) or
                (not self.high and level.price > self.rightExtremes[level.period] + self.levelAuthenticityThreshold.y * self.ATR(period)))then
            level:setAuthentic(false)
        end
    end
end

-----------------
-- Model Class --
-----------------

Model = {}
Model.__index = Model

function Model.new(source, askSource, periodsBack, minLinePoints, maxLinePoints, searchDepth, smoothings,
                    updateDepth, trendTimeFrames, levelAgeThreshold,
                    levelAuthenticityThreshold, ATRperiods, levelAggression)
    local self = setmetatable({}, Model)
    self.source = source
    self.askSource = askSource
    self.periodsBack = periodsBack or 300
    self.minLinePoints = minLinePoints or 3
    self.maxLinePoints = maxLinePoints or 25
    self.searchDepth = searchDepth or 25
    self.smoothings = smoothings or 10
    self.updateDepth = updateDepth or 15
    self.trendTimeFrames = trendTimeFrames or {"m30", "H3", "D1"}
    self.levelAgeThreshold = 1
    self.levelAuthenticityThreshold = levelAuthenticityThreshold or {x = 2, y = .1}
    --self.stopATRDistance = stopATRDistance or 1.5
    --self.limitATRDistance = limitATRDistance or 4.5*self.stopATRDistance
    self.ATRperiods = ATRperiods or Parameters.ATRperiods
    --self.levelAggression = levelAggression or .1
    self.initialized = false
    self.ATR = ATR.new(self.source, self.ATRperiods)
    self.trendManager = TrendManager.new(self.source:instrument(), self.trendTimeFrames)
    self.levelLists = {}
    self.levelLists[true] = LevelManager.new(self.source, true, self.periodsBack, self.minLinePoints, self.maxLinePoints,
                                            self.searchDepth, self.smoothings, self.updateDepth, self.levelAuthenticityThreshold, self.ATR)
    self.levelLists[false] = LevelManager.new(self.askSource, false, self.periodsBack, self.minLinePoints, self.maxLinePoints,
                                            self.searchDepth, self.smoothings, self.updateDepth, self.levelAuthenticityThreshold, self.ATR)
    return self
end

function Model:initialize()
    self:setStart()
    self.ATR:initialize()
    self.trendManager:initialize()
    for _, levelList in pairs(self.levelLists) do
        if levelList.initialized == false then
            levelList:initialize()
        end
    end
    --self.setup = {entry = nil, stop = nil, limit = nil}
    self.initialized = true
end

function Model:update()
    self.ATR:update()
    self.trendManager:update()
    for _, levelList in pairs(self.levelLists) do
        levelList:update()
    end
    --Timer.start("Update Setup")
    --self:updateSetup()
    --Timer.stop("Update Setup")
end

function Model:setStart(startPeriod, startDate)
    self.startPeriod = startPeriod or self.source:size() - 1 - self.periodsBack
    self.startDate = startDate or self.source:date(self.startPeriod)
    for _, levelList in pairs(self.levelLists) do
        levelList:setStart(self.startPeriod, self.startDate)
    end
end

--[[
function Model:sync()
    if not self.initialized or self.startDate == self.source:date(self.startPeriod) then
        return self
    else
        core.host:trace("syncing")
        Artist.clear()
        local newModel = Model.new(self.source, self.periodsBack, self.minLinePoints, self.maxLinePoints, self.searchDepth, self.smoothings, self.updateDepth)
        newModel:initialize()
        return newModel
    end
end
--]]


------------------
-- Candle Class --
------------------

Candle = {}
Candle.__index = Candle

function Candle.new(startPeriod, endPeriod, high, low, open, close)
    local self = setmetatable({}, Candle)
    self.startPeriod = startPeriod
    self.endPeriod = endPeriod
    self.high = high
    self.low = low
    self.open = open
    self.close = close
    return self
end

function Candle:color()
    return self.open <= self.close
end

function Candle.getCandle(source, period)
    if period < source:first() or period >= source:size() then
        return nil
    end
    return Candle.new(period, period, source.high[period], source.low[period], source.open[period], source.close[period])
end

function Candle.sum(candles, rightToLeft)
    local high = candles[1].high
    local low = candles[1].low
    for _,candle in pairs(candles) do
        high = math.max(candle.high, high)
        low = math.min(candle.low, low)
    end
    if not rightToLeft then
        return Candle.new(candles[1].startPeriod, candles[#candles].endPeriod, high, low,
            candles[1].open, candles[#candles].close)
    else
        return Candle.new(candles[#candles].startPeriod, candles[1].endPeriod, high, low,
            candles[#candles].open, candles[1].close)
    end
end



-----------------
-- Trend Class --
-----------------

Trend = {}
Trend.__index = Trend

function Trend.new(source)
    local self = setmetatable({}, Trend)
    self.source = source
    self.period = nil
    self.action = nil
    self.anchor = nil
    self.archive = nil
    self.initialized = false
    return self
end

function Trend:initialize()
    self.period = self.source:size() - 1
    self:calculateTrend()
    self.initialized = true
end

function Trend:update()
    local lastCandle = Candle.getCandle(self.source, self.period)
    if self.archive == nil or self.anchor == nil or self.action == nil then
        self:calculateTrend()
    elseif self.period == self.source:size() - 1 and lastCandle:color() == self.action:color() then
        self.action = Candle.sum({self.action, lastCandle}, false)
        self:updateBroken()
    else
        Timer.start("Calculate Trend")
        self:calculateTrend()
        self:calculateTrendLine()
        Timer.stop("Calculate Trend")
    end
end

function Trend:getParentCandle(period, rightToLeft)
    rightToLeft = rightToLeft or true
    local candle
    local candles = {}
    candles[1] = Candle.getCandle(self.source, period)
    if candles[1] == nil then
        return nil
    end
    local color = candles[1]:color()
    local k
    if rightToLeft then
        k = period - 1
        candle = Candle.getCandle(self.source, k)
        while candle ~= nil and candle:color() == color do
            table.insert(candles, candle)
            k = k - 1
            if k < self.source:first() then
                return nil
            end
            candle = Candle.getCandle(self.source, k)
        end
    else
        k = period + 1
        candle = Candle.getCandle(self.source, k)
        while candle ~= nil and candle:color() == color do
            table.insert(candles, candle)
            k = k + 1
            if k >= self.source:size() then
                return nil
            end
            candle = Candle.getCandle(self.source, k)
        end
    end
    return Candle.sum(candles, rightToLeft)
end

function Trend:calculateTrend()
    self.sideways = nil
    self.up = nil
    self.setup = nil
    self.broken = nil
    
    self.action = self:getParentCandle(self.source:size() - 1)
    if self.action == nil then
        return nil
    end
    self.anchor = self:getParentCandle(self.action.startPeriod - 1)
    if self.anchor == nil then
        return nil
    end
    self.archive = self:getParentCandle(self.anchor.startPeriod - 1)
    if self.archive == nil then
        return nil
    end
    local k
    if self.action:color() then
        if self.archive.open <= self.anchor.close then
            self.sideways = true
            self.up = true
            self.setup = false
        else
            self.sideways = false
            k = self.archive.startPeriod - 1
            while self.up == nil and k >= self.source:first() do
                if math.max(self.source.open[k], self.source.close[k]) > self.anchor.open then
                    self.up = false
                    self.setup = true
                elseif math.min(self.source.open[k], self.source.close[k]) < self.anchor.close then
                    self.up = true
                    self.setup = false
                else
                    k = k - 1
                end
            end
        end
        self.broken = self.action.close > self.anchor.open
    else
        if self.archive.open >= self.anchor.close then
            self.sideways = true
            self.up = false
            self.setup = false
        else
            self.sideways = false
            k = self.archive.startPeriod - 1
            while self.up == nil and k >= self.source:first() do
                if math.max(self.source.open[k], self.source.close[k]) > self.anchor.close then
                    self.up = false
                    self.setup = false
                elseif math.min(self.source.open[k], self.source.close[k]) < self.anchor.open then
                    self.up = true
                    self.setup = true
                else
                    k = k - 1
                end
            end
        end
        self.broken = self.action.close < self.anchor.open
    end
    if not self.sideways then
        local candle = self:getParentCandle(k)
        if candle ~= nil then
            self.previousExtreme = self.source:date(candle.startPeriod)
        else
            self.previousExtreme = nil
        end
    end
end

function Trend:updateBroken()
    if self.action:color() then
        self.broken = self.action.close > self.anchor.open
    else
        self.broken = self.action.close < self.anchor.open
    end
end

function Trend:calculateTrendLine()
    local high = "high"
    if self.up then
        high = "low"
    end
    self.LTFsource = self.LTFsource or Historian.getLTF(self.source:instrument())[high]
    if Historian.loading == 0 and self.previousExtreme ~= nil and self.previousExtreme > self.LTFsource:date(self.LTFsource:first()) then
        self.trendLine = Line.modelRange(core.findDate(self.LTFsource, self.previousExtreme), self.LTFsource:size() - 1, self.LTFsource)
    else
        self.trendLine = nil
    end
end

function Trend.__tostring(trend)
    if trend.sideways == nil or trend.up == nil or trend.setup == nil or trend.broken == nil then
        return ""
    end
    local str = ""
    if trend.sideways then
        str = "Sideways "
    end
    if trend.up then
        str = str .. "Up "
    else
        str = str .. "Down "
    end
    if trend.setup then
        str = str .. "Setup "
    end
    if trend.broken then
        str = str .. "Broken "
    end
    return string.sub(str, 1, -2)
end

------------------------
-- TrendManager Class --
------------------------

TrendManager = {}
TrendManager.__index = TrendManager

function TrendManager.new(instrument, timeFrames)
    local self = setmetatable({}, TrendManager)
    self.instrument = instrument
    self.timeFrames = timeFrames
    self.trends = {}
    for k,timeFrame in pairs(self.timeFrames) do
        self.trends[timeFrame] = Trend.new(Historian.getHistory(self.instrument, timeFrame))
    end
    self.LTF = TrendManager.minTimeFrame(self.timeFrames)
    self.initialized = false
    return self
end

function TrendManager:initialize()
    for _,trend in pairs(self.trends) do
        trend:initialize()
    end
    self.initialized = true
    Artist.drawLabel(tostring(self))
end

function TrendManager:update()
    for _,trend in pairs(self.trends) do
        trend:update()
    end
    Artist.drawLabel(tostring(self))
end

function TrendManager.minTimeFrame(timeFrames)
    local typeKey = {m = 1, H = 60, D = 60*24, W = 7*60*24, M = 30*60*24}
    local bestValue = nil
    local min = nil
    for k,timeFrame in pairs(timeFrames) do
        if min == nil then
            min = timeFrame
            bestValue = typeKey[string.sub(timeFrame, 1, 1)]*tonumber(string.sub(timeFrame, 2,-1))
        else
            value = typeKey[string.sub(timeFrame, 1, 1)]*tonumber(string.sub(timeFrame, 2,-1))
            if value < bestValue then
                min = timeFrame
                bestValue = value
            end
        end
    end
    return min
end

function TrendManager.__tostring(trendManager)
    local str = ""
    for timeFrame,trend in pairs(trendManager.trends) do
        str = str .. timeFrame .. ": " .. tostring(trend) .. "\n"
    end
    return str
end

---------------
-- ATR Class --
---------------

ATR = {}
ATR.__index = ATR

function ATR.new(source, N)
    local self = setmetatable({}, ATR)
    self.source = source
    self.N = N
    self.atr = {}
    self.initialized = false
    return self
end

function ATR:initialize(period)
    period = period or self.source:size() - 1
    if period - self.source:first() <= self.N + 1 then
        return
    end
    self.atr[period] = 0
    for k = period - self.N, period - 1 do
        self.atr[period] = self.atr[period] + self:range(k)
    end
    self.atr[period] = self.atr[period] / self.N
    if period == self.source:size() - 1 then
        self.initialized = true
    end
end  

function ATR:update()
    if not self.initialized then
        self:initialize()
    else
        local period = self.source:size() - 1
        if self.atr[period] == nil then
            self.atr[period] = self.atr[period - 1] + (self:range(period - 1) - self:range(period - self.N)) / self.N
        end
    end
end

function ATR:range(period)
    return self.source.high[period] - self.source.low[period]
end

function ATR.__call(self, period)
    period = period or self.source:size() - 1
    if self.atr[period] == nil then
        self:initialize(period)
    end
    return self.atr[period] or self()
end



-------------------
-- Analyst Class --
-------------------

Analyst = {}
Analyst.__index = Analyst
Analyst.boolTable = {}
Analyst.boolTable[true]  =  1
Analyst.boolTable[false] = -1

function Analyst.new(source, askSource, periodsBack, tradeLogger, levelAgeThreshold, levelATRSeparation,
                        stopATRDistance, limitATRDistance, levelAggression, LTFonly, entryLevelsBack, stopMoveMinLevelsBack)
    local self = setmetatable({}, Analyst)
    self.source = source
    self.askSource = askSource
    self.periodsBack = periodsBack or 300
    --self.tradeLogger = tradeLogger
    self.levelAgeThreshold = 1
    self.levelATRSeparation = levelATRSeparation or Parameters.levelATRSeparation
    self.stopATRDistance = stopATRDistance or Parameters.stopATRDistance
    self.stopMoveATRDistance = stopMoveATRDistance or Parameters.stopMoveATRDistance
    self.limitATRDistance = limitATRDistance or 3*self.stopATRDistance
    self.levelAggression = levelAggression or Parameters.levelAggression
    self.entryLevelsBack = entryLevelsBack or Parameters.entryLevelsBack
    self.stopMoveMinLevelsBack = stopMoveMinLevelsBack or Parameters.stopMoveMinLevelsBack
    self.LTFonly = LTFonly or true
    self.model = Model.new(self.source, self.askSource, self.periodsBack)
    --self.setup = {}
    --self.currentTrade = {}
    self.initialized = false
    return self
end

function Analyst:initialize()
    self.model:initialize()
    self.initialized = true
end

function Analyst:update()
    self.model:update()
    Timer.start("Update Setup")
    --self:updateSetup()
    Timer.stop("Update Setup")
end

function Analyst:getClosestLevels(n, high)

    function closerLevel(level, reference, high)
        if high then
            if level.authentic and ((reference == nil or reference.price > level.price) and
                                    level.period < self.source:size() - self.levelAgeThreshold) then
                return level
            else
                return reference
            end
        else
            if level.authentic and ((reference == nil or reference.price < level.price) and
                                    level.period < self.source:size() - self.levelAgeThreshold) then
                return level
            else
                return reference
            end
        end
    end
    
    local closestLevels = {}
    local newClosestLevel
    for _,level in pairs(self.model.levelLists[high].levels) do
        newClosestLevel = closerLevel(level, closestLevels[1], high)
        if newClosestLevel == level then
            for k=0,n-2 do
                closestLevels[n-k] = closestLevels[n-k-1]
            end
            closestLevels[1] = newClosestLevel
        end
    end
    
    return closestLevels
    
end

function Analyst:getSetup()

    local LTFtrend = self.model.trendManager.trends[self.model.trendManager.LTF]
    if not LTFtrend.setup or LTFtrend.broken then
        --self:replaceSetup()
        return
    end
    
    if not self.LTFonly then
        for _,trend in pairs(self.model.trendManager.trends) do
            if trend.up ~= LTFtrend.up then
                --self:replaceSetup()
                return
            end
        end
    end
    
    local high = not LTFtrend.up
    local closestLevel = self:getClosestLevels(self.entryLevelsBack, high)[self.entryLevelsBack]

    if closestLevel == nil or (high and closestLevel.price > LTFtrend.anchor.open) or
       (not high and closestLevel.price < LTFtrend.anchor.open) then
        return
    end
    
    local entry = closestLevel.price + Analyst.boolTable[LTFtrend.up] * self.levelAggression * self.model.ATR()
    local stop = closestLevel.price - Analyst.boolTable[LTFtrend.up] * self.stopATRDistance * self.model.ATR()

    return entry, stop, closestLevel.leftSlope, closestLevel.rightSlope
end

function Analyst:getStop(currentStop, entryPrice, long)
    if currentStop == nil then return end
    entryPrice = entryPrice or currentStop
    local closestLevels = self:getClosestLevels(5, not long)
    local n = self.stopMoveMinLevelsBack
    local newStop = closestLevels[n]
    
    if newStop == nil then
        return
    end
    
    while math.abs(newStop.price - closestLevels[1].price) < self.levelATRSeparation * self.model.ATR(closestLevels[1].period) and
                    closestLevels[n+1] ~= nil do
        n = n + 1
        newStop = closestLevels[n]
    end
    
    if math.abs(newStop.price - closestLevels[1].price) < self.levelATRSeparation * self.model.ATR(closestLevels[1].period) then
        return currentStop
    end
    
    local newStopPrice = newStop.price - Analyst.boolTable[long] * self.stopMoveATRDistance * self.model.ATR(newStop.period)
    
    if (newStopPrice - currentStop) * Analyst.boolTable[long] and (newStopPrice - entryPrice) * Analyst.boolTable[long] > 0 then
        return newStopPrice
    else
        return currentStop
    end
end



-----------------
-- Trade Class --
-----------------

Trade = {}
Trade.__index = Trade

function Trade.new(OrderID)
    local self = setmetatable({}, Trade)
    self.OpenOrderID = OrderID
    return self
end

function Trade:getRow(tableName)
    tableName = tableName or "Trades"
    local column
    if self.TradeID == nil then
        column = "OpenOrderID"
    else
        column = "TradeID"
    end
    local enum, row
    enum = core.host:findTable(tableName):enumerator()
    row = enum:next()
    while row ~= nil and row[column] ~= self[column] do
        row = enum:next()
    end
    return row
end
function Trade:draw()
    if Artist.on then
        if self.level ~= nil then
            Artist.removeLine(self.level.artistID)
            self.level = nil
        end
        local row = self:getRow()
        if row ~= nil then
            self.level = Level.new(trader.source:size() - 2, row.Open, true, true, 1)
            Artist.drawLevel(self.level, core.rgb(124,252,0))
        end
    end
end

-----------------
-- Order Class --
-----------------

Order = {Order = {}}
Order.Order.__index = Order.Order
Order.Order.PLACED = 0
Order.Order.PLACING = 1
Order.Order.EDITING = 2
Order.Order.CANCELLING = 3
Order.Order.CANCELLED = 4
Order.Order.EXECUTED = 5
Order.Order.FAILED = 6
Order.Order.buySell = {}
Order.Order.buySell[true] = "B"
Order.Order.buySell[false] = "S"
Order.Order.BStoBool = {}
Order.Order.BStoBool["B"] = true
Order.Order.BStoBool["S"] = false

function Order.Order.new(OfferID, AcctID)
    local self = setmetatable({}, Order.Order)
    self.OfferID = OfferID
    self.AcctID = AcctID
    return self
end

function Order.Order:getRow(tableName)
    tableName = tableName or "Orders"
    local tableColumn, selfColumn
    if self.OrderID == nil then
        selfColumn = "RequestID"
        tableColumn = "OpenOrderReqID"
    else
        selfColumn = "OrderID"
        tableColumn = "OrderID"
    end
    --[[
    if tableName ~= "Orders" then
        column = "OpenOrderReqID"
    end
    --]]
    local enum, row
    enum = core.host:findTable(tableName):enumerator()
    row = enum:next()
    while row ~= nil and (row[tableColumn] ~= self[selfColumn] or row.Type ~= self.Type) do
        row = enum:next()
    end
    return row
end

function Order.Order:edit(Rate)
    --assert(self:getRow() ~= nil, "Editing order, row is nil")
    
    if self:getRow() == nil then
        core.host:trace(self.Type .. ": Editing order, row is nil")
        local enum, row
        enum = core.host:findTable("Orders"):enumerator()
        row = enum:next()
        while row ~= nil and row.Type ~= self.Type do
            row = enum:next()
        end
        self.OrderID = row.OrderID

        assert(self.OrderID ~= nil, "Lost track of order")
    end
    
    if self.Rate == Rate then
        return true
    end
    
    local valuemap = core.valuemap()
    valuemap.Command = "EditOrder"
    valuemap.AcctID = self.AcctID
    valuemap.OrderID = self.OrderID
    valuemap.Rate = Rate
    
    local success, msg = terminal:execute(self.cookie, valuemap)
    if success then
        self.newRate = Rate
        self.status = Order.Order.EDITING
    else
        self.status = Order.Order.PLACED
    end
    
    return success
end

function Order.Order:cancel()
    if self.status ~= Order.Order.PLACED then
        return false
    end
    --assert(self:getRow() ~= nil, "Delete order, row is nil")
    
    if self:getRow() == nil then
        core.host:trace(self.Type .. ": Cancelling order, row is nil")
        local enum, row
        enum = core.host:findTable("Orders"):enumerator()
        row = enum:next()
        while row ~= nil and row.Type ~= self.Type do
            row = enum:next()
        end
        self.OrderID = row.OrderID

        assert(self.OrderID ~= nil, "Lost track of order")
    end
    
    local valuemap = core.valuemap()
    valuemap.Command = "DeleteOrder"
    valuemap.AcctID = self.AccountID
    valuemap.OrderID = self.OrderID
    
    local success, msg = terminal:execute(self.cookie, valuemap)
    if success then
        self.status = Order.Order.CANCELLING
    end
    return success
end

function Order.Order:requestProcessed(success, msg)
    assert(self.status == Order.Order.PLACING or self.status == Order.Order.CANCELLING or
            self.status == Order.Order.EDITING, "Unexpected status" .. tostring(self.status))
    if self.status == Order.Order.PLACING then
        if success then
            self.status = Order.Order.PLACED
            self.OrderID = msg
        else
            --(tostring(self.Type) .. "  Placing failed: " .. msg)
            self.status = Order.Order.FAILED
        end
    elseif self.status == Order.Order.EDITING then
        if success then
            self.status = Order.Order.PLACED
            self.Rate = self.newRate
            self.newRate = nil
        else
            --core.host:trace("Edit failed: " .. msg)
            self.status = Order.Order.FAILED
        end
    else
        if success then
            self.status = Order.Order.CANCELLED
        else
            --core.host:trace("Cancellation failed: " .. msg)
            self.status = Order.Order.FAILED
        end
    end
end

function Order.Order:draw()
    if Artist.on then
        if self.level ~= nil then
            Artist.removeLine(self.level.artistID)
            self.level = nil
        end
        if self.status == Order.Order.PLACED then
            self.level = Level.new(trader.source:size() - 2, self.Rate, true, true, 1)
            Artist.drawLevel(self.level, core.rgb(255,20,147))
        end
    end
end

function Order.Order.__tostring(order)
    return "ReqID: " .. tostring(order.RequestID)
            .. ", Rate: " .. tostring(order.Rate)
            .. ", Type: " .. tostring(order.Type)
            .. ", Status: " .. tostring(order.status)
end

--------------------
-- LE Order Class --
--------------------

Order.LE = setmetatable({}, Order.Order)
Order.LE.__index = Order.LE
Order.LE.__tostring = Order.Order.__tostring
Order.LE.Type = "LE"
Order.LE.cookie = 2

function Order.LE.new(OfferID, AcctID, Rate, Quantity, BuySell)
    local self = Order.Order.new(OfferID, AcctID)
    self = setmetatable(self, Order.LE)
    self.Rate = Rate
    self.BuySell = BuySell
    self.Quantity = Quantity
    return self
end

--------------------
-- SE Order Class --
--------------------

Order.SE = setmetatable({}, Order.Order)
Order.SE.__index = Order.SE
Order.SE.__tostring = Order.Order.__tostring
Order.SE.Type = "SE"
Order.SE.cookie = 3

function Order.SE.new(OfferID, AcctID, Rate)
    local self = Order.Order.new(OfferID, AcctID)
    self = setmetatable(self, Order.SE)
    self.Rate = Rate
    return self
end

---------------
-- ELS Class --
---------------

Order.ELS = {}
Order.ELS.__index = Order.ELS

function Order.ELS.new(OfferID, AcctID, Rate, RateLimit, RateStop, Quantity, extraLogColumns)
    local self = setmetatable({}, Order.ELS)
    self.entry = Order.LE.new(OfferID, AcctID, Rate, Quantity, Order.Order.buySell[RateStop < Rate])
    self.stop  = Order.SE.new(OfferID, AcctID, RateStop)
    self.extraLogColumns = extraLogColumns or {}
    self.extraLogColumns.stopLog = ""
    self.garbage = false
    self:place()
    return self
end

function Order.ELS:place()
    local valuemap = core.valuemap()
    valuemap.Command = "CreateOrder"
    valuemap.EntryLimitStop = "Y"
    valuemap.OrderType = "LE"
    valuemap.OfferID = self.entry.OfferID
    valuemap.AcctID = self.entry.AcctID
    valuemap.Quantity = self.entry.Quantity
    valuemap.BuySell = self.entry.BuySell
    valuemap.Rate = self.entry.Rate
    --valuemap.RateLimit = self.RateLimit
    valuemap.RateStop = self.stop.Rate
    
    local success, msg = terminal:execute(self.entry.cookie, valuemap)
    if success then
        self.entry.RequestID = msg
        self.stop.RequestID = msg
        self.entry.status = Order.Order.PLACING
        self.stop.status = Order.Order.PLACING
    else
        --self:cancel()
        self.status = Order.Order.FAILED
    end
end

function Order.ELS:edit(Rate, RateStop)
    if self.entry.status ~= Order.Order.PLACED and self.stop.status ~= Order.Order.PLACED then
        return false
    end
    -- If we are moving a buy order downward, we should to move the stop first so that
    -- we do not try to move our entry below the current stop.  Etc.
    if self.entry.BuySell then
        if Rate < self.entry.Rate then
            if self.stop:edit(RateStop) then
                return self.entry:edit(Rate)
            else
                return false
            end
        else
            if self.entry:edit(Rate) then
                return self.stop:edit(RateStop)
            else
                return false
            end
        end
    else
        if Rate > self.entry.Rate then
            if self.stop:edit(RateStop) then
                return self.entry:edit(Rate)
            else
                return false
            end
        else
            if self.entry:edit(Rate) then
                return self.stop:edit(RateStop)
            else
                return false
            end
        end
    end
end

function Order.ELS:moveStop(Rate)
    if self.extraLogColumns.stopLog ~= "" and Rate ~= self.stop.Rate then
        self.extraLogColumns.stopLog = self.extraLogColumns.stopLog .. " " .. tostring(Rate)
    end
    self.stop:edit(Rate)
end

function Order.ELS:cancel()
    if self.trade ~= nil then
        return
    end
    if self.entry.status ~= Order.Order.CANCELLING and self.entry.status ~= Order.Order.CANCELLED and self.entry.status ~= Order.Order.FAILED then
        self.entry:cancel()
    end
end

function Order.ELS:update()
    -- Mark as garbage if order has failed or been cancelled
    if self.entry.status == Order.Order.FAILED or self.entry.status == Order.Order.CANCELLED then
        self.garbage = true
    end
    -- Wait if order is busy
    if self.entry.status == Order.Order.PLACING or self.entry.status == Order.Order.EDITING or self.entry.status == Order.Order.CANCELLING or
        self.stop.status == Order.Order.PLACING or self.stop.status  == Order.Order.EDITING or self.stop.status  == Order.Order.CANCELLING then
        return
    elseif self.trade == nil then
        -- If order cannot be found, it may have been executed, so look for an active trade with this OrderID.
        if self.entry.status == Order.Order.PLACED and self.entry:getRow() == nil then
            self.trade = Trade.new(self.entry.OrderID)
            self.extraLogColumns.stopLog = tostring(self.stop.Rate)
            local row = self.trade:getRow() -- Looking for active trade
            if row == nil then
                -- If trade cannot be found, maybe it was both opened and closed between two ticks.
                -- Check for a closed trade with this OrderID.
                self.trade.closedRow = self.trade:getRow("Closed Trades")
                assert(self.trade.closedRow ~= nil, "Lost track of order")
                self.garbage = true
            else
                self.trade.TradeID = row.TradeID
                self.entry.status = Order.Order.EXECUTED
            end
        end
    else
        -- If in trade, but trade cannot be found, check for closed trade.
        if self.trade:getRow() == nil then
            self.trade.closedRow = self.trade:getRow("Closed Trades")
            assert(self.trade.closedRow ~= nil, "Lost track of order")
            self.garbage = true
        end
    end
    self.entry:draw()
    self.stop:draw()
    if self.trade ~= nil then
        self.trade:draw()
    end
end

function Order.ELS:updateExtraLogColumns(extraLogColumns)
    for k,v in pairs(extraLogColumns) do
        self.extraLogColumns[k] = v
    end
end

function Order.ELS:removeLines()
    if not Artist.on then return end
    if self.entry.level ~= nil and self.entry.level.artistID ~= nil then
        Artist.removeLine(self.entry.level.artistID)
    end
    if self.stop.level ~= nil and self.stop.level.artistID ~= nil then
        Artist.removeLine(self.stop.level.artistID)
    end
    if self.trade ~= nil and self.trade.level ~= nil and self.trade.level.artistID ~= nil then
        Artist.removeLine(self.trade.level.artistID)
    end
end

function Order.ELS:AsyncOperationFinished(cookie, success, msg)
    if cookie == self.entry.cookie then
        if (self.entry.status == Order.Order.PLACED or self.entry.status == Order.Order.FAILED) and self.stop.status == Order.Order.PLACING then
            self.stop:requestProcessed(success, msg)
        else
            self.entry:requestProcessed(success, msg)
        end
    else
        assert(cookie == self.stop.cookie, "Unrecognized cookie")
        self.stop:requestProcessed(success, msg)
    end
end

------------------
-- Trader Class --
------------------

Trader = {}
Trader.__index = Trader

function Trader.new(instrument, timeFrame, AcctID, periodsBack, riskAmount, lotSize, startHour, stopHour)
    local self = setmetatable({}, Trader)
    self.instrument = instrument
    self.timeFrame = timeFrame
    self.AcctID = AcctID
    self.periodsBack = periodsBack or 300
    self.OfferID = core.host:findTable("offers"):find("Instrument", self.instrument).OfferID
    self.lotSize = lotSize or Parameters.lotSize
    self.riskAmount = riskAmount or Parameters.riskAmount
    self.startHour = startHour or 3
    self.stopHour = stopHour or 11
    self.tradeLogger = Logger.new(self.instrument, self.timeFrame, true, "LMtradelog")
    self.log = {}
    self.initialized = false
    self.on = false
    return self
end


function Trader:turnOff()
    if self.ELS ~= nil then
        --core.host:trace("Cancelling to turn off trading")
        self.ELS:cancel()
    else
        core.host:trace("Off:   " .. core.formatDate(core.now()))
        Artist.clear()
        self.analyst = nil
        self.on = false
        self.initialized = false
    end
end

function Trader:turnOn()
    core.host:trace("On:    " .. core.formatDate(core.now()))
    local startDate = core.now() - Historian.periodsToDays(self.periodsBack, self.timeFrame) - 2
    --if self.source == nil then
        self.source = Historian.getHistory(self.instrument, self.timeFrame, nil, true, startDate)
        Artist.source = self.source
    --end
    --if self.askSource == nil then
        self.askSource = Historian.getHistory(self.instrument, self.timeFrame, nil, false, startDate)
    --end
    -- self.askSource = Historian.getHistory(self.source:instrument(), self.source:barSize(), nil, false, self.source:date(self.source:first()))
    self.analyst = Analyst.new(self.source, self.askSource, self.periodsBack, self.tradeLogger)
    self.initialized = false
    self.on = true
end

function Trader:onCondition()
    -- local currentDate = core.dateToTable(self.source:date(self.source:size() - 1))
    local currentDate = core.dateToTable(core.now())
    return 2 <= currentDate.wday and currentDate.wday <= 5 and
            self.startHour <= currentDate.hour and currentDate.hour <= self.stopHour
end

function Trader:initialize()
    --core.host:trace(tostring(trader.source:size()))
    if not self:onCondition() then
        self:turnOff()
    elseif self.on and Historian.loading == 0 and trader.source:size() > trader.periodsBack and trader.askSource:size() > trader.periodsBack then
        self.analyst:initialize()
        self.initialized = true
    end
end

function Trader:update()
    if self.on then
        if self.initialized then
            self.analyst:update()
            if self.ELS ~= nil then
                self.ELS:update()
                if self.ELS.garbage then
                    if self.ELS.trade ~= nil and self.ELS.trade.closedRow ~= nil then
                        self.tradeLogger:logTrade(self.ELS.trade.closedRow, self.ELS.extraLogColumns)
                    end
                    self.ELS:removeLines()
                    self.ELS = nil
                end
            end
            
            if self.ELS == nil or self.ELS.trade == nil then
                local Rate, RateStop, leftSlope, rightSlope = self.analyst:getSetup()
                if Rate ~= nil and RateStop ~= nil then
                    if self:onCondition() then
                        if self.ELS == nil then
                            self.ELS = Order.ELS.new(self.OfferID, self.AcctID, Rate, nil,
                                        RateStop, self:getTradeAmount(Rate, RateStop),
                                        self:getExtraLogColumns({leftSlope = leftSlope, rightSlope = rightSlope}))
                        else
                            if Order.Order.BStoBool[self.ELS.entry.BS] and RateStop < Rate or
                               not Order.Order.BStoBool[self.ELS.entry.BS] and RateStop > Rate then
                                if self.ELS:edit(Rate, RateStop) then
                                    self.ELS:updateExtraLogColumns(self:getExtraLogColumns({leftSlope = leftSlope, rightSlope = rightSlope}))
                                end
                            else
                                -- If the order is a buy order, but we are trying to place a sell order,
                                -- (or vice versa) we will not do this through an edit.  Instead, delete
                                -- the order. On the next update, if we still want to place this order, we will.
                                self.ELS:cancel()
                            end
                        end
                    end
                end
            else
                -- If in trade, look for opportunity to move stop.
                local newStop = self.analyst:getStop(self.ELS.stop.Rate, self.ELS.entry.Rate, self.ELS.entry.BuySell == "B")
                if newStop ~= nil then
                    self.ELS:moveStop(newStop)
                end
            end
            
            if not self:onCondition() and (self.ELS == nil or self.ELS.trade == nil) then
                self:turnOff()
            end
        else
            self:initialize()
        end
    else
        if self:onCondition() then
            self:turnOn()
        end
    end
end

function Trader:getTradeAmount(entry, stop)
    return self.lotSize * math.ceil(self.riskAmount / (self.lotSize * math.abs(entry - stop)))
end

function Trader:getExtraLogColumns(moreColumns)
    moreColumns = moreColumns or {}
    local columns = {}
    for timeFrame,trend in pairs(self.analyst.model.trendManager.trends) do
        columns["Trend_" .. tostring(timeFrame)] = tostring(trend)
        if trend.trendLine ~= nil then
            columns["Trendline.slope." .. tostring(timeFrame)] = tostring(trend.trendLine.slope)
            columns["Trendline.r." .. tostring(timeFrame)] = tostring(trend.trendLine.r)
        else
            columns["Trendline.slope." .. tostring(timeFrame)] = ""
            columns["Trendline.r." .. tostring(timeFrame)] = ""
        end
        
    end
    
    for k,v in pairs(moreColumns) do
        columns[k] = v
    end
    
    columns.ATR = tostring(self.analyst.model.ATR())
    return columns
end




------------------
-- Artist Class --
------------------

Artist = {}
Artist.__index = Artist
Artist.nLines = 0
Artist.on = true

function Artist.drawLine(line, color, style)
    if not Artist.on then return end
    if line == nil then return end
    local style = style or core.LINE_SOLID
    Artist.nLines = Artist.nLines + 1
    local color = color or core.rgb(0,0,255)
    core.host:execute("drawLine", Artist.nLines,
        Artist.source:date(line.from), line(line.from),
        Artist.source:date(line.to), line(line.to), color, style, 2)
    line.artistID = Artist.nLines
end

function Artist.drawLevel(level, color)
    if not Artist.on then return end
    if level.strength == 0 then return end
    Artist.nLines = Artist.nLines + 1
    if color == nil then
        if level.authentic then
            if level.high then
                color = core.rgb(0,191,255)
            else
                color = core.rgb(255,140,0)
            end
        else
            color = core.rgb(105,105,105)
        end
    end
    core.host:execute("drawLine", Artist.nLines,
        Artist.source:date(level.period - 1), level.price,
        Artist.source:date(level.period + 1), level.price, color, core.LINE_SOLID, 2)
    level.artistID = Artist.nLines
end

function Artist.drawLabel(str)
    if not Artist.on then return end
    core.host:execute("drawLabel1", 0, -5, core.CR_RIGHT, 0, core.CR_TOP, core.H_Left, core.V_Bottom, font, core.rgb(0,0,255), str)
end

function Artist.removeLine(id)
    if not Artist.on then return end
    core.host:execute("removeLine", id)
end

function Artist.clear()
    if not Artist.on then return end
    core.host:execute("removeAll")
    Artist.nLines = 0
end



---------------------
-- Historian Class --
---------------------

Historian = {}
Historian.__index = Historian
Historian.cookie = 1
Historian.loading = 0
Historian.typeKey = {m = 1/(60*24), H = 1/24, D = 1, W = 7, M = 30}
Historian.streams = {}

function Historian.getHistory(instrument, timeFrame, periodsBack, bid, fromDate)
    if bid == nil then
        bid = true
    end
    Historian.streams[instrument] = Historian.streams[instrument] or {}
    Historian.streams[instrument][timeFrame] = Historian.streams[instrument][timeFrame] or {}
    if Historian.streams[instrument][timeFrame][bid] == nil then
        local from, to
        Historian.loading = Historian.loading + 1
        to = core.now()
        if periodsBack == nil and fromDate == nil then
            from = 0
        else
            from = fromDate or to - Historian.periodsToDays(periodsBack, timeFrame)
        end
        Historian.streams[instrument][timeFrame][bid] = core.host:execute("getHistory", Historian.cookie, instrument, timeFrame, from, 0, bid)
    end
    return Historian.streams[instrument][timeFrame][bid]
end

function Historian.loaded()
    Historian.loading = Historian.loading - 1
end

function Historian.periodsToDays(periods, timeFrame)
    return periods * Historian.typeKey[string.sub(timeFrame, 1, 1)] * tonumber(string.sub(timeFrame, 2,-1))
end

function Historian.daysToPeriods(days, timeFrame)
    return days / (Historian.typeKey[string.sub(timeFrame, 1, 1)] * tonumber(string.sub(timeFrame, 2,-1)))
end
function Historian.getLTF(instrument)
    local timeFrames = {}
    for timeFrame, _ in pairs(Historian.streams[instrument]) do
        timeFrames[#timeFrames + 1] = timeFrame
    end
    if #timeFrames > 0 then
        return Historian.getHistory(instrument, Historian.minTimeFrame(timeFrames))
    else
        return nil
    end
end

function Historian.minTimeFrame(timeFrames)
    local typeKey = {m = 1, H = 60, D = 60*24, W = 7*60*24, M = 30*60*24}
    local bestValue = nil
    local min = nil
    for k,timeFrame in pairs(timeFrames) do
        if min == nil then
            min = timeFrame
            bestValue = typeKey[string.sub(timeFrame, 1, 1)]*tonumber(string.sub(timeFrame, 2,-1))
        else
            value = typeKey[string.sub(timeFrame, 1, 1)]*tonumber(string.sub(timeFrame, 2,-1))
            if value < bestValue then
                min = timeFrame
                bestValue = value
            end
        end
    end
    return min
end

---------------------
-- Timer Class --
---------------------

Timer = {}
Timer.__index = Timer
Timer.CLOCK = {}
Timer.times = {}
Timer.on = true

function Timer.start(name)
    if not Timer.on then return end
    assert(Timer.CLOCK[name] == nil, "Timer start error")
    Timer.CLOCK[name] = os.clock()
    if Timer.times[name] == nil then
        Timer.times[name] = 0
    end
end

function Timer.stop(name)
    if not Timer.on then return end
    assert(Timer.CLOCK[name] ~= nil, "Timer stop error")
    Timer.times[name] = Timer.times[name] + os.clock() - Timer.CLOCK[name]
    Timer.CLOCK[name] = nil
end

function Timer.print()
    if not Timer.on or next(Timer.times) == nil then return end
    for name, time in pairs(Timer.times) do
        print(name .. ":  " .. tostring(time))
    end
    print("")
end




------------------
-- LOGGER CLASS --
------------------

Logger = {}
Logger.__index = Logger
Logger.columns = {"OpenTime", "CloseTime", "BS", "Lot", "Open", "Close", "PL", "GrossPL", "Com", "Int"}
--Logger.extraColumns = {}

function Logger.new(instrument, timeFrame, enableLogging, logFileName)
	local self = setmetatable({}, Logger)
	if timeFrame == "m5" then
        timeFrame = ""
	end
	self.on = enableLogging
	self.lastOrderLog = {}
	self.extraColumns = {}
	self.logFilename = nil
	self.logFileHandle = nil
	if self.on then
		local nowTable = core.dateToTable(core.now())
		local nowFormatted = nowTable.year .. nowTable.month .. nowTable.day
		local paramString = tostring(10 * Parameters.stopATRDistance) ..
                                tostring(Parameters.entryLevelsBack) .. tostring(Parameters.stopMoveMinLevelsBack)
		self.logFilename = "C:/" .. string.sub(instrument, 1, 3) ..
                            string.sub(instrument, 5, 7) .."_"
        if timeFrame ~= "m5" then
            self.logFilename = self.logFilename .. timeFrame
        end
        self.logFilename = self.logFilename .. nowFormatted .. "_" .. paramString .. ".csv"
        
        -- Create separate file containing list of parameters used
        local paramFilename = string.sub(self.logFilename, 1, -5) .. "_Params.csv"
        local paramFileHandle, error = io.open(paramFilename, "w")
        local header = {}
        local params = {}
        for k,v in pairs(Parameters) do
            header[#header + 1] = k
            params[#params + 1] = v
        end
        paramFileHandle:write(table.concat(header, ",") .. "\n")
        paramFileHandle:write(table.concat(params, ",") .. "\n")
        paramFileHandle:close()
	end
	return self
end

function Logger:logTrade(tradeRow, extraColumns)
	if self.on then
        --extraColumns = extraColumns or self.lastOrderLog
        extraColumns = extraColumns or {}
        if (self.logFileHandle == nil) then
            for k,v in pairs(extraColumns) do
                table.insert(self.extraColumns, k)
            end
            
            local header = table.concat(Logger.columns, ",")
            if #self.extraColumns > 0 then
                header = header .. "," .. table.concat(self.extraColumns, ",")
            end
            
			self.logFileHandle, error = io.open(self.logFilename, "w")
			self.logFileHandle:write(header .. "\n")
			self.logFileHandle:close() -- Force a flush of the data for immediate use
		end
	
        local row = {}
        for k,col in pairs(Logger.columns) do
            row[k] = tradeRow[col]
        end

        for k,col in pairs(self.extraColumns) do
            row[k + #Logger.columns] = extraColumns[col]
        end

		self.logFileHandle, error = io.open(self.logFilename, "a")
		local dateTable = core.dateToTable(tradeRow.OpenTime)
		row[1] = string.format("%04d-%02d-%02dT%02d:%02d:%02d", dateTable.year,
                                            dateTable.month, dateTable.day, dateTable.hour, dateTable.min, dateTable.sec)
        dateTable = core.dateToTable(tradeRow.CloseTime)                                    
		row[2] = string.format("%04d-%02d-%02dT%02d:%02d:%02d", dateTable.year,
                                            dateTable.month, dateTable.day, dateTable.hour, dateTable.min, dateTable.sec)

		self.logFileHandle:write(table.concat(row, ",") .. "\n")
		self.logFileHandle:close() -- Force a flush of the data for immediate use
	end
end






----------------------------------------

----------------------------------------


function AsyncOperationFinished(cookie, success, msg)
    -- 1 : Historian.cookie
    -- 2 : Order processed/failed
    if cookie == Order.LE.cookie or cookie == Order.SE.cookie then
        if trader.ELS ~= nil then
            trader.ELS:AsyncOperationFinished(cookie, success, msg)
        end
    elseif cookie == Historian.cookie then
        Historian.loaded()
    end
end